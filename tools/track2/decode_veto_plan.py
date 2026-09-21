#!/usr/bin/env python3
"""Derive a plan that refuses to buy bytes with decode CPU.

`plan_deterministic` picks each column's (codec, encoding) by bytes alone,
because `advisor_policy.DECODE_MODELLED` is False and decode is therefore
invisible to the L1 search. On ClickBench `hits` that produced a clearly bad
trade: `DELTA_BYTE_ARRAY` on `Title` and `URL` buys 4.3% and 1.0% fewer bytes
on those two columns while costing 2.39x the decode CPU under parquet-mr --
and those are the two largest columns in the table.

This file does not fix the exchange rate between bytes and decode CPU. Fitting
that scalar needs an experiment that has not been run (see
`decode_calibrate.py`), and a plan should not depend on a number nobody has
identified. So the rule here is a *veto*, not a trade:

    an encoding is kept only if its measured decode cost is no worse than
    the same column's baseline encoding under the reading engine.

That needs no scale. An encoding that is both cheaper in bytes and cheaper to
decode survives; one that pays CPU for bytes is refused no matter how good the
byte side looks. `PLAIN` on the integer columns survives on exactly these
grounds -- it is 20-40% cheaper to decode *and* much smaller.

The reader that matters is the one that will read the candidate. UC1 writes
with PyArrow, but the benchmark reads with Spark, so `--reader parquet-mr` is
the default: `DELTA_BINARY_PACKED` looks decode-neutral under PyArrow (1.01)
and costs 1.53x under parquet-mr.

The output is a plan JSON that `write_layout_pyarrow.py` consumes unchanged.
Nothing is written to S3 here and no L1 score is recomputed: the vetoed plan
is a sibling candidate to be measured against the original, not a claim that
it wins.

Usage:
  python3 tools/track2/decode_veto_plan.py \
      --plan docs/.../e0_smoke_uc1/plan_deterministic.json \
      --decode-probe docs/.../e0_smoke_uc1/decode_probe.json \
      --layout-probe docs/.../e0_smoke_uc1/layout_probe.json \
      --out docs/.../e0_smoke_uc1/plan_deterministic.decode_veto.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import layout_actions as la  # noqa: E402

ENCODING_PREFIX = la.ENCODING_COLUMN_PREFIX


def load(path):
    with open(path) as fh:
        return json.load(fh)


def relative_cost(rates, physical_type, codec, encoding):
    """Measured decode cost of one (codec, encoding) on one physical type.

    `None` when the probe never measured the point; an unmeasured encoding is
    not vetoed, because refusing what has not been measured would silently
    prefer whatever happened to be probed.
    """
    table = (rates.get(physical_type) or {})
    entry = table.get(f"{codec}|{encoding}")
    if entry is None:
        return None
    return entry.get("relative_cost")


def column_codec(plan_actions, table, column):
    """The codec the plan gives this column, which sets the decode reference."""
    want = la.COMPRESSION_COLUMN_PREFIX + column
    for action in plan_actions:
        if action.get("canonical") == want and action.get("table") == table:
            return str(action["value"]).lower()
    for action in plan_actions:
        if action.get("canonical") == la.COMPRESSION and action.get("table") == table:
            return str(action["value"]).lower()
    return "snappy"


def veto(plan, decode_probe, layout_probe, reader):
    """Drop every encoding action whose decode cost exceeds its baseline."""
    rates = (decode_probe.get("rates") or {}).get(reader) or {}
    if not rates:
        raise SystemExit(f"decode probe has no rates for reader {reader!r}")
    columns_by_table = {
        name: (rec.get("columns") or {})
        for name, rec in ((layout_probe.get("tables") or {}).items())
    }

    kept, dropped, unmeasured = [], [], []
    for action in plan["actions"]:
        canonical = action.get("canonical", "")
        if not canonical.startswith(ENCODING_PREFIX):
            kept.append(action)
            continue
        column = canonical[len(ENCODING_PREFIX):]
        table = action.get("table")
        encoding = action["value"]
        rec = (columns_by_table.get(table) or {}).get(column) or {}
        physical_type = rec.get("physical_type")
        codec = column_codec(plan["actions"], table, column)
        cost = relative_cost(rates, physical_type, codec, encoding)
        base = relative_cost(rates, physical_type, codec, "baseline")
        if cost is None or base is None:
            unmeasured.append({"table": table, "column": column,
                               "encoding": encoding, "codec": codec,
                               "physical_type": physical_type})
            kept.append(action)
            continue
        if cost > base:
            dropped.append({"table": table, "column": column,
                            "encoding": encoding, "codec": codec,
                            "physical_type": physical_type,
                            "relative_cost": cost, "baseline_cost": base})
            continue
        kept.append(action)
    return kept, dropped, unmeasured


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--plan", required=True)
    ap.add_argument("--decode-probe", required=True)
    ap.add_argument("--layout-probe", required=True)
    ap.add_argument("--reader", default="parquet-mr",
                    choices=["parquet-mr", "pyarrow"],
                    help="engine that will READ the candidate (default the "
                         "benchmark's Spark reader, not the UC1 writer)")
    ap.add_argument("--plan-id", default=None)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    plan = load(args.plan)
    kept, dropped, unmeasured = veto(plan, load(args.decode_probe),
                                     load(args.layout_probe), args.reader)

    out = dict(plan)
    out["actions"] = kept
    out["plan_id"] = args.plan_id or f"{plan.get('plan_id', 'plan')}-decode-veto"
    out["generator"] = "decode_veto_plan"
    out["generated_at"] = datetime.now(timezone.utc).isoformat()
    out["derived_from"] = {
        "plan": args.plan,
        "plan_id": plan.get("plan_id"),
        "decode_probe": args.decode_probe,
        "reader": args.reader,
        "rule": "keep an encoding only if its measured decode relative_cost "
                "is <= the same column's baseline encoding under the reading "
                "engine; no bytes/CPU exchange rate is assumed",
        "dropped": dropped,
        "unmeasured_kept": unmeasured,
    }
    out.setdefault("explain", []).append(
        f"decode veto ({args.reader}): dropped {len(dropped)} encoding "
        f"action(s) that cost more decode than their baseline; "
        f"{len(unmeasured)} unmeasured encoding(s) left alone")

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as fh:
        json.dump(out, fh, indent=2)

    print(f"# decode veto  reader={args.reader}  "
          f"actions {len(plan['actions'])} -> {len(kept)}")
    for d in dropped:
        print(f"  dropped {d['table']}.{d['column']:<22} {d['encoding']:<22} "
              f"cost {d['relative_cost']:.3f} > baseline {d['baseline_cost']:.3f}")
    for u in unmeasured:
        print(f"  kept (unmeasured) {u['table']}.{u['column']} {u['encoding']}")
    print(f"  wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
