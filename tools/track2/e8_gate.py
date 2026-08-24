#!/usr/bin/env python3
"""Compare an E8 report.json against an E2 report.json (contract §4.1 / E8)."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone


def load_report(path):
    with open(path) as fh:
        return json.load(fh)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--e2", required=True)
    ap.add_argument("--e8", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--improve-frac", type=float, default=0.10)
    ap.add_argument("--cv", type=float, default=0.05)
    ap.add_argument("--query-regression", type=float, default=0.10)
    args = ap.parse_args()

    e2 = load_report(args.e2)
    e8 = load_report(args.e8)
    s2, s8 = e2["summary"], e8["summary"]
    e2_q = {q["query"]: q["median_s"] for q in s2["per_query"]}
    failures = []
    for q in s8["per_query"]:
        base = e2_q.get(q["query"])
        if not base:
            continue
        reg = q["median_s"] / base - 1.0
        if reg > args.query_regression:
            failures.append({
                "query": q["query"],
                "e8_median_s": q["median_s"],
                "e2_median_s": base,
                "regression": round(reg, 4),
            })
    improve = 1.0 - s8["median_s"] / s2["median_s"]
    walls = s8["end_to_end_s"]
    p95 = sorted(q["median_s"] for q in s8["per_query"])[int(0.95 * (len(s8["per_query"]) - 1))]
    e2_p95 = sorted(e2_q.values())[int(0.95 * (len(e2_q) - 1))]
    p99 = max(q["median_s"] for q in s8["per_query"])
    e2_p99 = max(e2_q.values())
    cv_pass = s8["cv"] < args.cv and s8["n_runs"] >= 5
    improve_pass = improve >= args.improve_frac
    guard_pass = not failures
    p95_ok = p95 <= e2_p95 * 1.05
    p99_ok = p99 <= e2_p99 * 1.05
    gate = cv_pass and improve_pass and guard_pass and p95_ok and p99_ok
    n_err = sum(len(q["errors"]) for q in s8["per_query"])
    report = {
        "experiment": "E8",
        "layout_id": e8.get("layout_id"),
        "data": e8.get("data"),
        "n_runs": s8["n_runs"],
        "n_query_errors": n_err,
        "end_to_end_s": s8["end_to_end_s"],
        "median_s": s8["median_s"],
        "baseline_median_s": s2["median_s"],
        "improve_frac": round(improve, 4),
        "improve_pass": improve_pass,
        "cv": s8["cv"],
        "cv_pass": cv_pass,
        "guardrails": {
            "single_query_regression_le_10pct": {
                "pass": guard_pass,
                "failures": failures,
            },
            "p95_p99_regression_le_5pct": {
                "pass": p95_ok and p99_ok,
                "e8_p95_s": p95,
                "e2_p95_s": e2_p95,
                "e8_p99_s": p99,
                "e2_p99_s": e2_p99,
            },
        },
        "gate_pass": gate and n_err == 0,
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(args.out, "w") as fh:
        json.dump(report, fh, indent=2)
    print(f"median {s8['median_s']:.1f}s vs E2 {s2['median_s']:.1f}s  "
          f"improve={improve*100:.1f}%  cv={s8['cv']*100:.2f}%  "
          f"guard_fail={len(failures)}  gate={'PASS' if report['gate_pass'] else 'FAIL'}")
    print(f"wrote {args.out}")
    return 0 if report["gate_pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
