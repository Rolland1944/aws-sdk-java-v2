#!/usr/bin/env python3
"""Fail a materialised layout whose measured geometry differs from L1's plan.

This is deliberately separate from writer footer verification.  The writer
proves canonical actions landed; this command proves the virtual footer used
for pricing describes the resulting file/RG/byte geometry closely enough to
benchmark.  Both gates are required before upload.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import advisor_catalog  # noqa: E402
import virtual_footer as vf  # noqa: E402
import whatif  # noqa: E402


def load(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dataset-snapshot", required=True,
                    help="baseline snapshot that L1 priced")
    ap.add_argument("--access-profile", required=True)
    ap.add_argument("--layout-probe", required=True)
    ap.add_argument("--plan", required=True)
    ap.add_argument("--candidate-snapshot", required=True)
    ap.add_argument("--table", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    catalog = advisor_catalog.load(args.dataset_snapshot, args.access_profile)
    whatif.bind_catalog(catalog)
    vf.bind_probe(load(args.layout_probe))
    plan = load(args.plan)
    actual = ((load(args.candidate_snapshot).get("geometry") or {})
              .get(args.table) or {})
    if not actual:
        raise SystemExit(f"candidate snapshot lacks geometry for {args.table}")
    predicted = vf.predict_geometry(args.table, plan)
    ok, failures = whatif.geometry_matches(predicted, actual)
    report = {
        "table": args.table,
        "plan_id": plan.get("plan_id") or plan.get("candidate_id"),
        "predicted": {key: predicted.get(key) for key in (
            "n_files", "n_rg", "compressed_bytes", "n_scan_units")},
        "measured": {key: actual.get(key) for key in (
            "files", "n_rg", "compressed_bytes", "n_scan_units")},
        "pass": ok,
        "failures": failures,
    }
    with open(args.out, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2)
        fh.write("\n")
    print(json.dumps(report, indent=2))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
