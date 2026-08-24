#!/usr/bin/env python3
"""Score M2 canary: L1 ranking vs measured compressed-workload wall-clock.

Validation set V = {baseline} ∪ measured candidate reports. Gate (contract E5):
L1's top-3 of the full grid contains the measured-best of V, OR the L1-selected
candidate's measured cost is within 5% of that measured best (selection regret).

Compressed queries (DB2 low, X=60%): 21,15,9,17,8,1,18,7,10,12.

Usage:
  python3 tools/track2/m2_gate.py \
      --ranked .../ranked_measured_cross_cloud.json \
      --per-query-baseline .../e2_baseline/per_query.csv \
      --measured .../m2_cand_best/report.json \
      --out .../m2_gate.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import advisor_policy  # noqa: E402

COMPRESSED = [21, 15, 9, 17, 8, 1, 18, 7, 10, 12]


def compressed_sum_from_csv(path, queries=COMPRESSED):
    med = advisor_policy.load_per_query_medians(path)
    return sum(med[q] for q in queries if q in med), med


def compressed_sum_from_report(path, queries=COMPRESSED):
    with open(path) as fh:
        rep = json.load(fh)
    by_q = {}
    errors = {}
    for run in rep.get("runs") or []:
        for q in run.get("queries") or []:
            if q.get("error"):
                errors.setdefault(q["query"], []).append(q["error"][:240])
                continue
            by_q.setdefault(q["query"], []).append(q["wall_s"])
    import statistics as st
    med = {q: st.median(vs) for q, vs in by_q.items()}
    missing = [q for q in queries if q not in med]
    if missing:
        return None, med, rep.get("layout_id"), {
            "missing_or_failed": missing, "errors": errors,
        }
    return sum(med[q] for q in queries), med, rep.get("layout_id"), {
        "missing_or_failed": [], "errors": errors,
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ranked", required=True)
    ap.add_argument("--per-query-baseline", required=True)
    ap.add_argument("--measured", nargs="+", default=[],
                    help="report.json from run_benchmark on candidate layouts")
    ap.add_argument("--l1-pick", default="p-none_f-1GB_rg-128MB_s-l_shipdate")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    ranked = json.load(open(args.ranked))
    top3 = [r["candidate_id"] for r in ranked["top10"][:3]]
    base_sum, base_med = compressed_sum_from_csv(args.per_query_baseline)
    rows = [{"id": "baseline", "measured_s": round(base_sum, 3), "source": "e2"}]
    for path in args.measured:
        if not os.path.exists(path):
            rows.append({"id": path, "measured_s": None, "error": "missing"})
            continue
        s, med, lid, extra = compressed_sum_from_report(path)
        row = {"id": lid or path, "measured_s": None if s is None else round(s, 3),
               "source": path, "per_query": med}
        row.update(extra)
        if s is None:
            row["error"] = "incomplete compressed workload (timeouts or missing queries)"
        rows.append(row)

    measured = [r for r in rows if r.get("measured_s") is not None]
    if not measured:
        raise SystemExit("no measured layouts")
    best = min(measured, key=lambda r: r["measured_s"])
    pick = next((r for r in measured if r["id"] == args.l1_pick), None)
    # Contract E5: L1 top-3 contains the measured-best of V, or selection regret ≤5%.
    in_top3 = best["id"] in set(top3)
    regret = None
    if pick and best:
        regret = (pick["measured_s"] - best["measured_s"]) / best["measured_s"]
    gate_pass = bool(in_top3 or (regret is not None and regret <= 0.05))
    out = {
        "compressed_queries": COMPRESSED,
        "l1_top3": top3,
        "l1_pick": args.l1_pick,
        "measured": rows,
        "measured_best": best["id"],
        "measured_best_s": best["measured_s"],
        "baseline_compressed_s": round(base_sum, 3),
        "in_l1_top3": in_top3,
        "selection_regret": None if regret is None else round(regret, 4),
        "gate_pass": gate_pass,
        "note": ("Incomplete compressed-workload reports (timeouts / missing "
                 "queries) are dropped. n=1 is ranking evidence, not the E2 "
                 "5-run CV gate."),
    }
    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)
    with open(args.out, "w") as fh:
        json.dump(out, fh, indent=2)
    print("# M2 gate")
    print(f"  baseline compressed {base_sum:.1f}s")
    print(f"  measured best       {best['id']} {best['measured_s']:.1f}s")
    print(f"  L1 pick in V        {pick['measured_s'] if pick else 'NOT MEASURED'}")
    print(f"  top3 hit            {in_top3}  regret={regret}")
    print(f"  gate                {'PASS' if gate_pass else 'FAIL / pending'}")
    return 0 if gate_pass else 1


if __name__ == "__main__":
    raise SystemExit(main())
