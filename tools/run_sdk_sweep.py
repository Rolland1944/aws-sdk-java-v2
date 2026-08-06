#!/usr/bin/env python3
"""Run the SDK-side policy-headroom sweep.

This script deliberately does not run during normal Maven tests. It drives the
manually-triggered AdaptiveReaderSystemBenchmark with a fixed cache budget and a
fixed aggressiveness work point for each process invocation, then writes
machine-readable summaries under access_report/.

Two objectives are reported side by side, because they rank policies
differently:

  bytes: remote bytes only.
  cost:  remoteGets * RTT + remoteBytes / BW. With 50ms RTT and 100MiB/s the
         RTT term dominates the byte term by more than an order of magnitude on
         the mixed holdout trace, so a byte-minimising oracle optimises the
         minor term.

The cost objective is computed from the zero-RTT counters, so it charges one RTT
per GET. That is exact at prefetchDepth 0, and pessimistic for speculative GETs
at higher depths since those overlap in the real reader. It is used to *choose*
oracle maps; the latency phase with injected sleeps is the ground truth.

Examples:
  python3 tools/run_sdk_sweep.py bytes --warmup 0 --iters 1
  python3 tools/run_sdk_sweep.py latency
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "services-custom/s3-adaptive-range-reader"
BENCHMARK_CSV = MODULE / "target/s3arr-benchmark/results-v2.csv"
TRACE = ROOT / "traces/mixed_holdout.csv"
REPORT_DIR = ROOT / "access_report"

MIB = 1024.0 * 1024.0

BUDGETS = (128, 256, 512, 1024)
WORKPOINTS = ((1, 0), (1, 1), (4, 4), (8, 8))
POLICIES = ("s3a_random", "template_locality", "s3a_prefetch", "template_multimodal")
PREFIXES = ("tpch", "clickbench", "fmnist", "sift", "emb_real", "lastfm", "taxi", "mm")
OBJECTIVES = ("bytes", "cost")

# These planners ignore prefetchBlockSize/prefetchDepth, so one run per budget
# covers every work point.
AGGRESSIVENESS_INVARIANT = ("s3a_random", "template_multimodal")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("bytes", "latency"))
    parser.add_argument("--mvn", default="mvn", help="Maven executable (default: mvn)")
    parser.add_argument("--trace", type=Path, default=TRACE)
    parser.add_argument("--budgets", type=int, nargs="+", default=BUDGETS)
    parser.add_argument("--workpoints", nargs="+", default=None,
                        help="work points as blockMiB:depth (default: 1:0 1:1 4:4 8:8)")
    parser.add_argument("--objectives", nargs="+", choices=OBJECTIVES, default=list(OBJECTIVES))
    parser.add_argument("--warmup", type=int, default=None)
    parser.add_argument("--iters", type=int, default=None)
    parser.add_argument("--rtt-ms", type=float, default=50.0)
    parser.add_argument("--bw-mibps", type=float, default=100.0)
    return parser.parse_args()


def workpoints_of(args: argparse.Namespace) -> tuple[tuple[int, int], ...]:
    if not args.workpoints:
        return WORKPOINTS
    parsed = []
    for spec in args.workpoints:
        block, _, depth = spec.partition(":")
        parsed.append((int(block), int(depth)))
    return tuple(parsed)


def label_for(budget: int, block: int, depth: int, kind: str) -> str:
    return f"headroom_b{budget}_block{block}_depth{depth}_{kind}"


def run_benchmark(args: argparse.Namespace, label: str, budget: int, block: int, depth: int,
                  extra_properties: dict[str, str]) -> list[dict[str, str]]:
    command = [
        args.mvn, "-q", "-pl", "services-custom/s3-adaptive-range-reader",
        "-Djapicmp.skip=true", "-Dcheckstyle.skip=true", "-Dspotbugs.skip=true",
        "-Dtest=AdaptiveReaderSystemBenchmark", "test",
        f"-Ds3arr.trace={args.trace.resolve()}",
        f"-Ds3arr.label={label}",
        f"-Ds3arr.cacheBudgetMiB={budget}",
        f"-Ds3arr.prefetchBlockMiB={block}",
        f"-Ds3arr.prefetchDepth={depth}",
        "-Ds3arr.apps=1",
    ]
    if args.warmup is not None:
        command.append(f"-Ds3arr.warmup={args.warmup}")
    if args.iters is not None:
        command.append(f"-Ds3arr.iters={args.iters}")
    for key, value in extra_properties.items():
        command.append(f"-D{key}={value}")

    print("+", " ".join(command), flush=True)
    subprocess.run(command, cwd=ROOT, check=True)
    return read_rows_for_label(label)


def read_rows_for_label(label: str) -> list[dict[str, str]]:
    if not BENCHMARK_CSV.exists():
        raise FileNotFoundError(f"Benchmark output missing: {BENCHMARK_CSV}")
    with BENCHMARK_CSV.open(newline="") as handle:
        rows = [row for row in csv.DictReader(handle) if row["label"] == label]
    if not rows:
        raise RuntimeError(f"Benchmark did not write rows for label {label}")
    return rows


def cost_seconds(row: dict[str, str], rtt_ms: float, bw_mibps: float) -> float:
    gets = float(row["remoteGets"])
    mib = float(row["remoteBytes"]) / MIB
    seconds = gets * (rtt_ms / 1000.0)
    if bw_mibps > 0:
        seconds += mib / bw_mibps
    return seconds


def enrich(rows: list[dict[str, str]], budget: int, block: int, depth: int, kind: str,
           args: argparse.Namespace) -> list[dict[str, str]]:
    return [{**row,
             "budgetMiB": str(budget),
             "prefetchBlockMiB": str(block),
             "prefetchDepth": str(depth),
             "case": kind,
             "costSeconds": f"{cost_seconds(row, args.rtt_ms, args.bw_mibps):.4f}"}
            for row in rows]


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else []
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(f"wrote {path}")


def all_scope(rows: list[dict[str, str]]) -> dict[str, str]:
    matches = [row for row in rows if row["scope"] == "all"]
    if len(matches) != 1:
        raise RuntimeError(f"Expected one all-scope row, got {len(matches)}")
    return matches[0]


def metric_of(row: dict[str, str], objective: str, args: argparse.Namespace) -> float:
    if objective == "bytes":
        return float(row["remoteBytes"])
    return cost_seconds(row, args.rtt_ms, args.bw_mibps)


def prefix_metrics(rows: list[dict[str, str]], objective: str,
                   args: argparse.Namespace) -> dict[str, float]:
    return {row["prefix"]: metric_of(row, objective, args)
            for row in rows if row["scope"] == "prefix"}


def encode_oracle_map(policies: dict[str, str]) -> str:
    return ",".join(f"{prefix}:{policy}" for prefix, policy in policies.items())


def force_rows(args: argparse.Namespace, budget: int, block: int, depth: int, first_workpoint: bool,
               rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    result: dict[str, list[dict[str, str]]] = {}
    for policy in POLICIES:
        if policy in AGGRESSIVENESS_INVARIANT and not first_workpoint:
            continue
        label = label_for(budget, block, depth, f"forced_{policy}")
        measured = run_benchmark(args, label, budget, block, depth, {"s3arr.forcePolicy": policy})
        rows.extend(enrich(measured, budget, block, depth, f"forced:{policy}", args))
        result[policy] = measured
    return result


def measure_oracle(args: argparse.Namespace, budget: int, block: int, depth: int,
                   available: dict[str, list[dict[str, str]]], objective: str,
                   rows: list[dict[str, str]]
                   ) -> tuple[dict[str, str], list[dict[str, str]], bool]:
    """Build the prefix->policy table by argmin, then measure it in the real shared cache.

    Returns the final table, its measured rows, and whether coordinate descent
    had to move the table away from the plain argmin.
    """
    by_prefix = argmin_table(available, objective, args)

    label = label_for(budget, block, depth, f"oracle_{objective}")
    measured = run_benchmark(args, label, budget, block, depth,
                             {"s3arr.oracleMap": encode_oracle_map(by_prefix)})
    rows.extend(enrich(measured, budget, block, depth, f"oracle_perworkload:{objective}", args))

    static_best = min(metric_of(all_scope(available[policy]), objective, args) for policy in POLICIES)
    if metric_of(all_scope(measured), objective, args) <= static_best:
        return by_prefix, measured, False

    # The per-prefix argmin was measured with a uniform policy, while the oracle
    # replays a mixed policy against one shared cache. If that interference
    # loses to the best static policy, run one coordinate-descent pass so the
    # reported ceiling stays constructive.
    print(f"[refine] {objective} oracle lost to static at {budget}MiB ({block}MiB, depth {depth})",
          flush=True)
    for prefix in PREFIXES:
        candidates: dict[str, list[dict[str, str]]] = {}
        for policy in POLICIES:
            candidate = dict(by_prefix)
            candidate[prefix] = policy
            candidate_label = label_for(budget, block, depth, f"refine_{objective}_{prefix}_{policy}")
            candidate_rows = run_benchmark(args, candidate_label, budget, block, depth,
                                           {"s3arr.oracleMap": encode_oracle_map(candidate)})
            rows.extend(enrich(candidate_rows, budget, block, depth,
                               f"oracle_refinement:{objective}", args))
            candidates[policy] = candidate_rows
        by_prefix[prefix] = min(
            candidates, key=lambda policy: metric_of(all_scope(candidates[policy]), objective, args))

    label = label_for(budget, block, depth, f"oracle_{objective}_refined")
    measured = run_benchmark(args, label, budget, block, depth,
                             {"s3arr.oracleMap": encode_oracle_map(by_prefix)})
    rows.extend(enrich(measured, budget, block, depth, f"oracle_perworkload:{objective}", args))
    return by_prefix, measured, True


def run_bytes(args: argparse.Namespace) -> None:
    rows: list[dict[str, str]] = []
    oracle_maps: dict[str, dict[str, dict[str, str]]] = {}
    workpoints = workpoints_of(args)

    for budget in args.budgets:
        label = label_for(budget, workpoints[0][0], workpoints[0][1], "passthrough")
        measured = run_benchmark(args, label, budget, workpoints[0][0], workpoints[0][1],
                                 {"s3arr.selector": "passthrough"})
        for block, depth in workpoints:
            rows.extend(enrich(measured, budget, block, depth, "passthrough", args))

        invariant: dict[str, list[dict[str, str]]] = {}
        for index, (block, depth) in enumerate(workpoints):
            forced = force_rows(args, budget, block, depth, index == 0, rows)
            if index == 0:
                invariant = {policy: forced[policy] for policy in AGGRESSIVENESS_INVARIANT}
            else:
                for policy in AGGRESSIVENESS_INVARIANT:
                    rows.extend(enrich(invariant[policy], budget, block, depth,
                                       f"forced:{policy}", args))

            for selector in ("template_auto", "decision_tree"):
                selector_label = label_for(budget, block, depth, selector)
                selector_rows = run_benchmark(args, selector_label, budget, block, depth,
                                              {"s3arr.selector": selector})
                rows.extend(enrich(selector_rows, budget, block, depth,
                                   f"selector:{selector}", args))

            available = {**invariant, **forced}
            maps_for_point: dict[str, dict[str, str]] = {}
            # Two objectives often pick the same table; replay it once.
            measured_seeds: dict[str, tuple[dict[str, str], list[dict[str, str]]]] = {}
            for objective in args.objectives:
                seed = encode_oracle_map(argmin_table(available, objective, args))
                if seed in measured_seeds:
                    table, reused = measured_seeds[seed]
                    maps_for_point[objective] = table
                    rows.extend(enrich(reused, budget, block, depth,
                                       f"oracle_perworkload:{objective}", args))
                    continue
                table, measured, refined = measure_oracle(
                    args, budget, block, depth, available, objective, rows)
                maps_for_point[objective] = table
                if not refined:
                    # Refinement is objective-driven, so a refined table is not
                    # reusable across objectives.
                    measured_seeds[seed] = (table, measured)
            oracle_maps[f"{budget}:{block}:{depth}"] = maps_for_point

    write_csv(REPORT_DIR / "sdk_sweep_bytes.csv", rows)
    (REPORT_DIR / "sdk_sweep_oracles.json").write_text(
        json.dumps(oracle_maps, indent=2, sort_keys=True) + "\n")
    print(f"wrote {REPORT_DIR / 'sdk_sweep_oracles.json'}")
    summarize(rows, args, REPORT_DIR / "sdk_sweep_summary.csv")


def argmin_table(available: dict[str, list[dict[str, str]]], objective: str,
                 args: argparse.Namespace) -> dict[str, str]:
    per_policy_prefix = {policy: prefix_metrics(available[policy], objective, args)
                         for policy in POLICIES}
    return {prefix: min(POLICIES, key=lambda policy: per_policy_prefix[policy][prefix])
            for prefix in PREFIXES}


def summarize(rows: list[dict[str, str]], args: argparse.Namespace, path: Path) -> None:
    """Collapse the sweep into the three bounds per (budget, work point, objective)."""
    all_rows = [row for row in rows if row["scope"] == "all"]
    summary: list[dict[str, str]] = []

    keys = sorted({(int(r["budgetMiB"]), int(r["prefetchBlockMiB"]), int(r["prefetchDepth"]))
                   for r in all_rows})
    for budget, block, depth in keys:
        cell = [r for r in all_rows
                if (int(r["budgetMiB"]), int(r["prefetchBlockMiB"]), int(r["prefetchDepth"]))
                == (budget, block, depth)]

        def pick(case: str) -> dict[str, str] | None:
            matches = [r for r in cell if r["case"] == case]
            return matches[-1] if matches else None

        forced = {policy: pick(f"forced:{policy}") for policy in POLICIES}
        forced = {policy: row for policy, row in forced.items() if row is not None}
        passthrough = pick("passthrough")
        template_auto = pick("selector:template_auto")
        decision_tree = pick("selector:decision_tree")

        for objective in args.objectives:
            oracle_pw = pick(f"oracle_perworkload:{objective}")
            if not forced or oracle_pw is None:
                continue
            static_policy = min(forced, key=lambda p: metric_of(forced[p], objective, args))
            static_value = metric_of(forced[static_policy], objective, args)
            pw_value = metric_of(oracle_pw, objective, args)

            def rel(row: dict[str, str] | None) -> str:
                if row is None:
                    return ""
                return f"{100.0 * (metric_of(row, objective, args) - pw_value) / pw_value:+.2f}"

            summary.append({
                "budgetMiB": str(budget),
                "prefetchBlockMiB": str(block),
                "prefetchDepth": str(depth),
                "objective": objective,
                "unit": "bytes" if objective == "bytes" else "seconds",
                "oracleStaticPolicy": static_policy,
                "oracleStatic": f"{static_value:.4f}",
                "oraclePerWorkload": f"{pw_value:.4f}",
                "adaptivityGapPct": f"{100.0 * (static_value - pw_value) / pw_value:+.2f}",
                "passthroughVsOraclePct": rel(passthrough),
                "templateAutoVsOraclePct": rel(template_auto),
                "decisionTreeVsOraclePct": rel(decision_tree),
            })

    if not summary:
        return
    write_csv(path, summary)
    print("\n=== three bounds per (budget, work point, objective) ===")
    header = (f"{'budget':>7} {'block':>6} {'depth':>6} {'objective':>10} {'staticPolicy':>20} "
              f"{'gap(static-pw)':>15} {'passthrough':>12} {'template_auto':>14} {'tree':>9}")
    print(header)
    for entry in summary:
        print(f"{entry['budgetMiB']:>7} {entry['prefetchBlockMiB']:>6} {entry['prefetchDepth']:>6} "
              f"{entry['objective']:>10} {entry['oracleStaticPolicy']:>20} "
              f"{entry['adaptivityGapPct']:>14}% {entry['passthroughVsOraclePct']:>11}% "
              f"{entry['templateAutoVsOraclePct']:>13}% {entry['decisionTreeVsOraclePct']:>8}%")


def best_workpoint(rows: list[dict[str, str]], budget: int, objective: str,
                   args: argparse.Namespace) -> tuple[int, int]:
    """The work point whose best measured configuration is best under this objective."""
    cell: dict[tuple[int, int], float] = {}
    for row in rows:
        if row["scope"] != "all" or row["budgetMiB"] != str(budget):
            continue
        key = (int(row["prefetchBlockMiB"]), int(row["prefetchDepth"]))
        value = metric_of(row, objective, args)
        cell[key] = min(cell.get(key, value), value)
    if not cell:
        raise RuntimeError(f"No sweep rows for budget {budget}MiB")
    return min(cell, key=lambda key: cell[key])


def run_latency(args: argparse.Namespace) -> None:
    bytes_path = REPORT_DIR / "sdk_sweep_bytes.csv"
    oracle_path = REPORT_DIR / "sdk_sweep_oracles.json"
    if not bytes_path.exists() or not oracle_path.exists():
        raise FileNotFoundError("Run the bytes phase first to create oracle maps.")
    with bytes_path.open(newline="") as handle:
        byte_rows = list(csv.DictReader(handle))
    oracle_maps = json.loads(oracle_path.read_text())

    rows: list[dict[str, str]] = []
    common = {"s3arr.rttMs": str(args.rtt_ms), "s3arr.bwMiBps": str(args.bw_mibps)}

    for budget in args.budgets:
        # The cost objective is the one the latency phase is testing, so it
        # selects the work point; (1,1) is kept as the current SDK default.
        selected = {best_workpoint(byte_rows, budget, "cost", args), (1, 1)}
        for block, depth in sorted(selected):
            forced = [row for row in byte_rows
                      if row["budgetMiB"] == str(budget)
                      and row["prefetchBlockMiB"] == str(block)
                      and row["prefetchDepth"] == str(depth)
                      and row["scope"] == "all" and row["case"].startswith("forced:")]
            if not forced:
                continue
            best = min(forced, key=lambda row: metric_of(row, "cost", args))
            worst = max(forced, key=lambda row: metric_of(row, "cost", args))

            cases = {
                "static_best": {**common, "s3arr.forcePolicy": best["case"].split(":", 1)[1]},
                "static_worst": {**common, "s3arr.forcePolicy": worst["case"].split(":", 1)[1]},
            }
            for kind, properties in cases.items():
                label = label_for(budget, block, depth, f"latency_{kind}")
                measured = run_benchmark(args, label, budget, block, depth, properties)
                rows.extend(enrich(measured, budget, block, depth, kind, args))

            # Both oracle tables are replayed when they differ, since the whole
            # point is whether the objective changes the answer.
            tables = oracle_maps.get(f"{budget}:{block}:{depth}", {})
            replayed: dict[str, str] = {}
            for objective, table in tables.items():
                encoded = encode_oracle_map(table)
                if encoded in replayed:
                    reused = read_rows_for_label(replayed[encoded])
                    rows.extend(enrich(reused, budget, block, depth,
                                       f"oracle_perworkload:{objective}", args))
                    continue
                label = label_for(budget, block, depth, f"latency_oracle_{objective}")
                measured = run_benchmark(args, label, budget, block, depth,
                                         {**common, "s3arr.oracleMap": encoded})
                rows.extend(enrich(measured, budget, block, depth,
                                   f"oracle_perworkload:{objective}", args))
                replayed[encoded] = label

            for selector in ("template_auto", "decision_tree"):
                label = label_for(budget, block, depth, f"latency_{selector}")
                measured = run_benchmark(args, label, budget, block, depth,
                                         {**common, "s3arr.selector": selector})
                rows.extend(enrich(measured, budget, block, depth, f"selector:{selector}", args))

    write_csv(REPORT_DIR / "sdk_sweep_latency.csv", rows)


def main() -> int:
    args = parse_args()
    if not args.trace.exists():
        raise FileNotFoundError(args.trace)
    if args.phase == "bytes":
        run_bytes(args)
    else:
        run_latency(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
