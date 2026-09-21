#!/usr/bin/env python3
"""P3-0: per-query optimistic bound from fixed D1 cells.

The bound pretends each query could have used a different cache history at
no switching cost. It is an upper bound on a query-aware controller, not a
claim about the online policy.
"""

from __future__ import annotations

import argparse
import json
import sys


def load(path):
    with open(path) as fh:
        return json.load(fh)


def cell_query_medians(cell):
    return {q["query"]: q["median_s"] for q in cell.get("per_query") or []}


def from_cell_summaries(cells):
    fixed = [c for c in cells if not c.get("d1_adaptive")]
    if not fixed:
        fixed = list(cells)
    by_id = {c["id"]: c for c in fixed}
    queries = sorted({
        q["query"]
        for c in fixed
        for q in c.get("per_query") or []
    })
    optimistic = []
    chosen = []
    for qnr in queries:
        options = []
        for cell in fixed:
            med = cell_query_medians(cell).get(qnr)
            if med is None:
                continue
            options.append((med, cell["id"]))
        if not options:
            continue
        best_s, best_id = min(options)
        optimistic.append({"query": qnr, "median_s": best_s, "cell": best_id})
        chosen.append(best_id)
    opt_sum = sum(q["median_s"] for q in optimistic)
    ranked = sorted(fixed, key=lambda c: (c.get("median_s") is None, c.get("median_s"), c["id"]))
    winner = ranked[0] if ranked else None
    winner_sum = winner["median_s"] if winner else None
    cell_100 = by_id.get("100")
    gaps = {}
    for cell in fixed:
        wall = cell.get("median_s")
        if wall and opt_sum:
            gaps[cell["id"]] = {
                "median_s": wall,
                "vs_optimistic": wall / opt_sum - 1.0,
            }
    coverage = None
    rho = None
    if winner and winner.get("d1_r_h") and winner.get("d1_target_budget"):
        coverage = winner["d1_target_budget"] / max(winner["d1_r_h"], 1)
    if winner and winner.get("d1_u_h") and winner.get("d1_target_budget"):
        rho = winner["d1_u_h"] / max(winner["d1_target_budget"], 1)
    return {
        "winner": winner["id"] if winner else None,
        "winner_median_s": winner_sum,
        "optimistic_sum_s": opt_sum if optimistic else None,
        "optimistic_gap_vs_winner": (
            (winner_sum / opt_sum - 1.0) if winner_sum and opt_sum else None),
        "gap_vs_100": (
            (cell_100["median_s"] / winner_sum - 1.0)
            if cell_100 and winner_sum else None),
        "per_query": optimistic,
        "chosen_cells": chosen,
        "cell_gaps": gaps,
        "winner_coverage": coverage,
        "winner_rho": rho,
        "working_sets": [
            {
                "id": c["id"],
                "u_h": c.get("d1_u_h"),
                "r_h": c.get("d1_r_h"),
                "rd_p90": c.get("d1_rd_p90"),
                "target_budget": c.get("d1_target_budget"),
                "peak_heap_bytes": c.get("peak_heap_bytes"),
                "gets": c.get("gets"),
                "remote_bytes": c.get("remote_bytes"),
                "median_s": c.get("median_s"),
            }
            for c in fixed
        ],
    }


def from_report(report):
    return from_cell_summaries(report.get("results") or [])


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("report", help="p2_report.json or p3_report.json")
    args = ap.parse_args()
    report = load(args.report)
    result = from_report(report)
    json.dump(result, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
