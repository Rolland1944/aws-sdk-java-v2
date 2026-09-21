#!/usr/bin/env python3
"""Rerank the existing L1 search grid by IO vs IO+decode.

Does not invent candidates. The grid is exactly what plan_deterministic
already enumerates: the Cartesian product of the six option lists. The only
question is whether the cheapest point under t_io is still the cheapest
under t_cost, and by how much.

Usage:
  python3 tools/track2/decode_rerank.py \
      --results docs/.../e0_smoke_uc1 \
      --sysconst docs/.../sysconst.json --regime same_region_m5d
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import access_profile  # noqa: E402
import advisor_catalog  # noqa: E402
import advisor_policy as policy  # noqa: E402
import dataset_snapshot  # noqa: E402
import layout_actions as la  # noqa: E402
import plan_deterministic as plan  # noqa: E402
import virtual_footer as vf  # noqa: E402
import whatif  # noqa: E402


def enumerate_legal(catalog, table, regime, vectored, probe, dims, writer):
    """Every L0-legal point the planner would price, with both scores."""
    notes = []
    orders = plan.order_options(catalog, table, "column_order" in dims, notes)
    codecs = plan.compression_options(catalog, table, probe, dims, notes)
    files = catalog.file_options(table) if "file_size" in dims else [("baseline", None)]
    rgs = catalog.rg_options(table) if "row_group" in dims else [("baseline", None)]
    pages = plan.page_options(catalog, table, "page" in dims, notes)
    patterns = catalog.patterns_for(table)

    points = []
    rejected = 0
    for order_label, order in orders:
        for codec_label, codec_actions in codecs:
            for page_label, page_bytes in pages:
                for file_label, file_bytes in files:
                    for rg_label, rg_bytes in rgs:
                        actions = list(codec_actions)
                        if order:
                            actions.append({"canonical": la.COLUMN_ORDER,
                                            "value": list(order), "table": table})
                        if page_bytes:
                            actions.append({"canonical": la.PAGE_SIZE,
                                            "value": int(page_bytes), "table": table})
                        if file_bytes:
                            actions.append({"canonical": la.TARGET_FILE_SIZE,
                                            "value": int(file_bytes), "table": table})
                        if rg_bytes:
                            actions.append({"canonical": la.ROW_GROUP_SIZE,
                                            "value": int(rg_bytes), "table": table})
                        cand = {"candidate_id": (
                            f"{order_label}|{codec_label}|{page_label}|"
                            f"{file_label}|{rg_label}"),
                            "actions": actions}
                        ok, _, _ = whatif.l0_check(cand, writer=writer)
                        if not ok:
                            rejected += 1
                            continue
                        ev = whatif.evaluate_workload(cand, patterns, regime,
                                                      vectored)
                        points.append({
                            "id": cand["candidate_id"],
                            "column_order": order_label,
                            "compression": codec_label,
                            "page": page_label,
                            "file": file_label,
                            "row_group": rg_label,
                            "t_io_s": ev["t_io_s"],
                            "t_decode_s": ev["t_decode_s"],
                            "t_cost_s": ev["t_cost_s"],
                            "decode_core_s": ev["decode_core_s"],
                            "decode_priced": ev["decode_priced"],
                            "bytes_gib": ev["bytes_gib"],
                            "ranged_gets": ev["ranged_gets"],
                        })
    return points, rejected, notes


def winner(points, key):
    return min(points, key=lambda p: (p[key], p["t_io_s"], p["id"]))


def rank_of(points, cid, key):
    ordered = sorted(points, key=lambda p: (p[key], p["t_io_s"], p["id"]))
    for i, p in enumerate(ordered, 1):
        if p["id"] == cid:
            return i
    return None


def axis_breakdown(points, key):
    """Best score on each axis, holding nothing fixed -- just the min by label."""
    out = {}
    for axis in ("column_order", "compression", "page", "file", "row_group"):
        by = {}
        for p in points:
            rec = by.setdefault(p[axis], {"n": 0, "best": None})
            rec["n"] += 1
            if rec["best"] is None or p[key] < rec["best"][key]:
                rec["best"] = p
        out[axis] = {label: {"n": rec["n"],
                             "best_id": rec["best"]["id"],
                             "t_io_s": rec["best"]["t_io_s"],
                             "t_cost_s": rec["best"]["t_cost_s"]}
                     for label, rec in by.items()}
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--results", required=True)
    ap.add_argument("--sysconst", required=True)
    ap.add_argument("--regime", default="same_region_m5d")
    ap.add_argument("--writer", choices=("pyarrow", "parquet-mr"),
                    default="pyarrow")
    ap.add_argument("--decode-reader", choices=vf.DECODE_READERS,
                    default="pyarrow")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    snap = dataset_snapshot.DatasetSnapshot(
        json.load(open(os.path.join(args.results, "dataset_snapshot.json"))))
    prof = access_profile.AccessProfile(
        json.load(open(os.path.join(args.results, "access_profile.json"))))
    cat = advisor_catalog.AdvisorCatalog(snap, prof)
    whatif.bind_catalog(cat)
    with open(os.path.join(args.results, "layout_probe.json")) as fh:
        vf.bind_probe(json.load(fh))
    with open(os.path.join(args.results, "decode_probe.json")) as fh:
        vf.bind_decode_profile(json.load(fh), reader=args.decode_reader,
                               rate_scale=policy.DECODE_RATE_SCALE)
    with open(args.sysconst) as fh:
        sysc = json.load(fh)
    regime = sysc["regimes"][args.regime]
    vectored = sysc.get("vectored") or {}

    table = cat.largest_table()
    dims = set(plan.SEARCHED_AXES)
    points, rejected, notes = enumerate_legal(
        cat, table, regime, vectored, vf.probe, dims, args.writer)
    if not points:
        raise SystemExit("no legal points")
    if any(not p["decode_priced"] for p in points):
        n = sum(1 for p in points if not p["decode_priced"])
        raise SystemExit(f"{n}/{len(points)} points unpriced for decode")

    io_w = winner(points, "t_io_s")
    cost_w = winner(points, "t_cost_s")
    all_base = [p for p in points if p["id"].count("baseline") == 5]
    if not all_base:
        raise SystemExit("baseline point missing from the legal grid")
    base = all_base[0]

    same = io_w["id"] == cost_w["id"]
    io_gain = (base["t_io_s"] - io_w["t_io_s"]) / base["t_io_s"]
    cost_of_io = cost_w["t_cost_s"]  # noqa: F841
    extra = (io_w["t_cost_s"] - cost_w["t_cost_s"]) / max(io_w["t_cost_s"], 1e-9)

    print(f"# decode rerank  {len(points)} legal / {rejected} L0-rejected")
    print(f"  rate_scale={policy.DECODE_RATE_SCALE:.4f}  "
          f"DECODE_MODELLED={policy.DECODE_MODELLED}")
    print(f"  baseline  t_io={base['t_io_s']:.1f}s  t_decode={base['t_decode_s']:.1f}s  "
          f"t_cost={base['t_cost_s']:.1f}s")
    print()
    print(f"  winner by t_io    {io_w['id']}")
    print(f"                    t_io={io_w['t_io_s']:.1f}s  "
          f"t_decode={io_w['t_decode_s']:.1f}s  t_cost={io_w['t_cost_s']:.1f}s  "
          f"({100 * io_gain:+.1f}% vs baseline IO)")
    print(f"  winner by t_cost  {cost_w['id']}")
    print(f"                    t_io={cost_w['t_io_s']:.1f}s  "
          f"t_decode={cost_w['t_decode_s']:.1f}s  t_cost={cost_w['t_cost_s']:.1f}s")
    print()
    if same:
        print("  verdict  SAME winner. Decode does not change the pick.")
        print("           Formal L1 should keep ranking on t_io.")
    else:
        print(f"  verdict  WINNER CHANGES.")
        print(f"           IO-winner ranks #{rank_of(points, io_w['id'], 't_cost_s')} "
              f"under t_cost; cost-winner ranks "
              f"#{rank_of(points, cost_w['id'], 't_io_s')} under t_io.")
        print(f"           Theoretical extra saving if L1 ranked on t_cost: "
              f"{io_w['t_cost_s'] - cost_w['t_cost_s']:.2f}s "
              f"({100 * extra:.2f}% of the IO-winner's t_cost).")

    print("\n  compression-axis best under each score")
    print(f"  {'label':24} {'best t_io':>10} {'its t_cost':>10}  "
          f"{'best t_cost':>11} {'its t_io':>9}")
    labels = sorted({p["compression"] for p in points})
    for label in labels:
        subset = [p for p in points if p["compression"] == label]
        a, b = winner(subset, "t_io_s"), winner(subset, "t_cost_s")
        mark = ""
        if label == io_w["compression"]:
            mark += "  <- IO winner"
        if label == cost_w["compression"] and label != io_w["compression"]:
            mark += "  <- cost winner"
        print(f"  {label:24} {a['t_io_s']:10.1f} {a['t_cost_s']:10.1f}  "
              f"{b['t_cost_s']:11.1f} {b['t_io_s']:9.1f}{mark}")

    print("\n  top 5 by t_io")
    for i, p in enumerate(sorted(points, key=lambda x: x["t_io_s"])[:5], 1):
        print(f"    {i}. {p['t_io_s']:7.1f}s io  {p['t_cost_s']:7.1f}s cost  {p['id']}")
    print("  top 5 by t_cost")
    for i, p in enumerate(sorted(points, key=lambda x: x["t_cost_s"])[:5], 1):
        print(f"    {i}. {p['t_cost_s']:7.1f}s cost {p['t_io_s']:7.1f}s io    {p['id']}")

    for note in notes:
        if "L1 chose" not in note:
            print(f"  note  {note}")

    doc = {
        "n_legal": len(points),
        "n_l0_rejected": rejected,
        "rate_scale": policy.DECODE_RATE_SCALE,
        "same_winner": same,
        "baseline": base,
        "winner_t_io": io_w,
        "winner_t_cost": cost_w,
        "extra_saving_s": None if same else round(io_w["t_cost_s"] - cost_w["t_cost_s"], 3),
        "extra_saving_frac": None if same else round(extra, 4),
        "axis_best_t_io": axis_breakdown(points, "t_io_s"),
        "axis_best_t_cost": axis_breakdown(points, "t_cost_s"),
        "points": points,
        "notes": notes,
    }
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(doc, fh, indent=2)
        print(f"\n  out {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
