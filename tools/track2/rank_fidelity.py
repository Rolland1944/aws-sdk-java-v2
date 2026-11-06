#!/usr/bin/env python3
"""Summarise a frozen L1 rank-fidelity experiment.

This intentionally evaluates ordering, not claimed absolute-time accuracy:
L1's t_io_s is compared with median measured end-to-end workload wall time.
The input must contain one report per candidate in every frozen random block.
"""

from __future__ import annotations

import argparse
import csv
import itertools
import json
import math
import statistics
from pathlib import Path


def load(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def average_ranks(values, lower_is_better=True):
    indices = sorted(range(len(values)), key=lambda i: values[i],
                     reverse=not lower_is_better)
    ranks = [0.0] * len(values)
    index = 0
    while index < len(indices):
        end = index + 1
        while end < len(indices) and values[indices[end]] == values[indices[index]]:
            end += 1
        rank = (index + 1 + end) / 2.0
        for pos in indices[index:end]:
            ranks[pos] = rank
        index = end
    return ranks


def pearson(x, y):
    mx, my = statistics.mean(x), statistics.mean(y)
    dx = sum((value - mx) ** 2 for value in x)
    dy = sum((value - my) ** 2 for value in y)
    if not dx or not dy:
        return 0.0
    return sum((a - mx) * (b - my) for a, b in zip(x, y)) / math.sqrt(dx * dy)


def spearman(predicted, measured):
    return pearson(average_ranks(predicted), average_ranks(measured))


def kendall_tau_b(x, y):
    concordant = discordant = tied_x = tied_y = 0
    for left in range(len(x)):
        for right in range(left + 1, len(x)):
            dx = (x[left] > x[right]) - (x[left] < x[right])
            dy = (y[left] > y[right]) - (y[left] < y[right])
            if not dx and not dy:
                continue
            if not dx:
                tied_x += 1
            elif not dy:
                tied_y += 1
            elif dx == dy:
                concordant += 1
            else:
                discordant += 1
    den = math.sqrt((concordant + discordant + tied_x) *
                    (concordant + discordant + tied_y))
    return (concordant - discordant) / den if den else 0.0


def exact_p_values(predicted, measured):
    """Two-sided permutation p values over all n! assignments (n=8 here)."""
    observed_s = spearman(predicted, measured)
    observed_k = kendall_tau_b(predicted, measured)
    n_s = n_k = total = 0
    for permutation in itertools.permutations(measured):
        if abs(spearman(predicted, permutation)) >= abs(observed_s) - 1e-12:
            n_s += 1
        if abs(kendall_tau_b(predicted, permutation)) >= abs(observed_k) - 1e-12:
            n_k += 1
        total += 1
    return {
        "permutations": total,
        "spearman_two_sided_p": n_s / total,
        "kendall_tau_b_two_sided_p": n_k / total,
    }


def relative_error(predicted, actual):
    if predicted is None or actual is None:
        return None
    if actual == 0:
        return None
    return (predicted - actual) / actual


def report_data(path):
    report = load(path)
    runs = report.get("runs") or []
    if len(runs) != 1:
        raise ValueError(f"{path}: expected exactly one run")
    run = runs[0]
    queries = run.get("queries") or []
    errors = [q for q in queries if q.get("error")]
    if errors:
        raise ValueError(f"{path}: {len(errors)} query errors")
    hashes = {}
    rows = {}
    for query in queries:
        query_id = str(query.get("query"))
        value = query.get("result_sha256")
        if not value:
            raise ValueError(f"{path}: query {query_id} lacks result_sha256")
        hashes[query_id] = value
        rows[query_id] = query.get("rows")
    return run["query_sum_s"], hashes, rows


def validate_results(records):
    """Require stable results, while recording layout-sensitive float queries.

    Exact integer and ordered-group queries must hash identically everywhere.
    Queries whose SQL uses order-dependent floating-point aggregation may differ
    between physical layouts, but each layout must reproduce one hash and one
    row count on all five rounds.
    """
    if not records:
        raise ValueError("no reports")
    query_ids = sorted(records[0]["hashes"], key=lambda q: int(q))
    exact = []
    layout_dependent = []
    for query_id in query_ids:
        row_counts = {record["rows"][query_id] for record in records}
        if len(row_counts) != 1:
            raise ValueError(f"query {query_id} row counts differ: {sorted(row_counts)}")
        by_candidate = {}
        for record in records:
            by_candidate.setdefault(record["candidate_id"], set()).add(
                record["hashes"][query_id])
        unstable = [candidate for candidate, values in by_candidate.items()
                    if len(values) != 1]
        if unstable:
            raise ValueError(f"query {query_id} is unstable within {unstable}")
        groups = {}
        for candidate, values in by_candidate.items():
            groups.setdefault(next(iter(values)), []).append(candidate)
        if len(groups) == 1:
            exact.append(query_id)
        else:
            layout_dependent.append({
                "query": query_id,
                "rows": next(iter(row_counts)),
                "n_hash_groups": len(groups),
                "groups": [sorted(candidates) for candidates in groups.values()],
            })
    return {
        "n_queries": len(query_ids),
        "exact_hash_queries": exact,
        "layout_dependent_hash_queries": layout_dependent,
        "row_counts_consistent": True,
        "within_candidate_hashes_stable": True,
    }


def summary_for(predicted, measured):
    p_rank = average_ranks(predicted)
    m_rank = average_ranks(measured)
    best_l1 = min(range(len(predicted)), key=lambda i: (predicted[i], i))
    best_measured = min(range(len(measured)), key=lambda i: (measured[i], i))
    return {
        "spearman_rho": spearman(predicted, measured),
        "kendall_tau_b": kendall_tau_b(predicted, measured),
        "predicted_ranks": p_rank,
        "measured_ranks": m_rank,
        "l1_top1_index": best_l1,
        "measured_best_index": best_measured,
        "l1_top1_in_measured_top3": m_rank[best_l1] <= 3.0,
        "regret": ((measured[best_l1] - measured[best_measured]) /
                   measured[best_measured]),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--protocol", required=True)
    ap.add_argument("--root", required=True,
                    help="experiment output root containing grid/ and layouts/")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    protocol = load(args.protocol)
    root = Path(args.root)
    candidates = protocol["candidates"]
    rounds = protocol["randomization"]["rounds"]
    candidate_ids = [entry["candidate_id"] for entry in candidates]
    if len(candidate_ids) != 8 or len(rounds) != 5:
        raise SystemExit("this protocol must contain exactly 8 arms and 5 rounds")

    measured = {candidate_id: [] for candidate_id in candidate_ids}
    records = []
    for block in rounds:
        round_number = block["round"]
        if sorted(block["candidate_order"]) != sorted(candidate_ids):
            raise SystemExit(f"round {round_number} is not a complete block")
        for candidate_id in candidate_ids:
            report = root / "grid" / candidate_id / f"run-{round_number}" / "report.json"
            if not report.is_file():
                raise SystemExit(f"missing required report: {report}")
            value, query_hashes, query_rows = report_data(report)
            measured[candidate_id].append(value)
            records.append({
                "candidate_id": candidate_id,
                "round": round_number,
                "hashes": query_hashes,
                "rows": query_rows,
            })
    consistency = validate_results(records)

    predicted = [entry["l1"]["t_io_s"] for entry in candidates]
    medians = [statistics.median(measured[candidate_id])
               for candidate_id in candidate_ids]
    primary = summary_for(predicted, medians)
    significance = exact_p_values(predicted, medians)

    per_candidate = []
    geometry_errors = {"files": [], "n_rg": [], "compressed_bytes": []}
    for index, (entry, median) in enumerate(zip(candidates, medians)):
        candidate_id = entry["candidate_id"]
        actual_snapshot = root / "layouts" / candidate_id / "dataset_snapshot.json"
        if not actual_snapshot.is_file():
            raise SystemExit(f"missing actual layout snapshot: {actual_snapshot}")
        actual = (load(actual_snapshot).get("geometry") or {}).get(protocol["table"])
        if not actual:
            raise SystemExit(f"{actual_snapshot}: missing {protocol['table']} geometry")
        predicted_geometry = entry["virtual_geometry"]
        actual_geometry = {
            "files": actual.get("files"),
            "n_rg": actual.get("n_rg"),
            "compressed_bytes": actual.get("compressed_bytes"),
        }
        errors = {metric: relative_error(predicted_geometry.get(metric),
                                         actual_geometry[metric])
                  for metric in geometry_errors}
        for metric, error in errors.items():
            if error is not None:
                geometry_errors[metric].append(abs(error))
        values = measured[candidate_id]
        mean = statistics.mean(values)
        per_candidate.append({
            "candidate_id": candidate_id,
            "source_grid_id": entry["source_grid_id"],
            "selection_reason": entry["selection_reason"],
            "l1_rank_t_io": entry["l1"]["rank_t_io"],
            "l1_t_io_s": entry["l1"]["t_io_s"],
            "predicted_rank": primary["predicted_ranks"][index],
            "measured_rank": primary["measured_ranks"][index],
            "round_query_sum_s": values,
            "median_query_sum_s": median,
            "mean_query_sum_s": mean,
            "cv": statistics.stdev(values) / mean if len(values) > 1 and mean else 0.0,
            "virtual_geometry": predicted_geometry,
            "actual_geometry": actual_geometry,
            "geometry_relative_error": errors,
        })

    loo = []
    for omit in range(5):
        values = [statistics.median(measured[candidate_id][:omit] +
                                    measured[candidate_id][omit + 1:])
                  for candidate_id in candidate_ids]
        rec = summary_for(predicted, values)
        loo.append({
            "omitted_round": omit + 1,
            "spearman_rho": rec["spearman_rho"],
            "kendall_tau_b": rec["kendall_tau_b"],
            "l1_top1_in_measured_top3": rec["l1_top1_in_measured_top3"],
            "regret": rec["regret"],
        })

    geometry_summary = {
        metric: {
            "median_absolute_percentage_error": statistics.median(values) * 100,
            "max_absolute_percentage_error": max(values) * 100,
        } for metric, values in geometry_errors.items() if values
    }
    output = {
        "schema_version": 1,
        "study": protocol["study"],
        "measurement": {
            "metric": "median of five Track1-off full-workload query_sum_s values",
            "n_candidates": len(candidates),
            "n_rounds": len(rounds),
            "n_queries_per_run": consistency["n_queries"],
            "result_consistency": consistency,
        },
        "rank_fidelity": {
            "spearman_rho": primary["spearman_rho"],
            "kendall_tau_b": primary["kendall_tau_b"],
            "top1_in_measured_top3": primary["l1_top1_in_measured_top3"],
            "l1_selected_candidate": candidate_ids[primary["l1_top1_index"]],
            "best_measured_candidate": candidate_ids[primary["measured_best_index"]],
            "selection_regret": primary["regret"],
            **significance,
        },
        "geometry_error": geometry_summary,
        "leave_one_round_out": loo,
        "candidates": per_candidate,
    }
    out = Path(args.out) if args.out else root / "rank_fidelity_summary.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")

    csv_path = out.with_suffix(".csv")
    fields = ["candidate_id", "source_grid_id", "l1_rank_t_io", "l1_t_io_s",
              "predicted_rank", "measured_rank", "median_query_sum_s",
              "mean_query_sum_s", "cv"]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows([{field: row[field] for field in fields}
                           for row in per_candidate])
    print(f"Spearman={primary['spearman_rho']:.4f} "
          f"Kendall-tau-b={primary['kendall_tau_b']:.4f} "
          f"top1@3={primary['l1_top1_in_measured_top3']} "
          f"regret={primary['regret']:.2%}")
    print(f"summary={out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
