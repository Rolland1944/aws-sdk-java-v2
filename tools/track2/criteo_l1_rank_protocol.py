#!/usr/bin/env python3
"""Freeze the Criteo L1 rank-fidelity candidate pool before any write occurs.

The pool is selected exclusively from the L0-legal virtual grid.  It contains
the source baseline, the L1 winner, four one-axis diagnostic candidates, and
two deliberately poor legal points.  Its result is an executable protocol:
plans, evidence hashes, virtual geometry and the five random run orders.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import access_profile  # noqa: E402
import advisor_catalog  # noqa: E402
import dataset_snapshot  # noqa: E402
import decode_rerank  # noqa: E402
import plan_deterministic as plan  # noqa: E402
import virtual_footer as vf  # noqa: E402
import whatif  # noqa: E402


AXES = ("column_order", "compression", "page", "file", "row_group")


def load_json(path):
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def action_key(point):
    return json.dumps(point["actions"], sort_keys=True, separators=(",", ":"))


def strictly_worse(points, point):
    """Return the requested point followed by same-slice, worse alternatives."""
    ordered = sorted(points, key=lambda p: (p["t_io_s"], p["id"]))
    return ordered[ordered.index(point):]


def with_global_fallback(slice_points, point, all_points):
    """Prefer a diagnostic slice, then preserve the duplicate fallback rule.

    Some one-axis Criteo slices have only their baseline point left after L0:
    e.g. the baseline file geometry is the only legal file-only layout.  The
    protocol must still have eight distinct physical layouts, so after that
    duplicate it takes the next worse global legal point rather than quietly
    measuring the baseline twice.
    """
    preferred = strictly_worse(slice_points, point)
    seen = {p["id"] for p in preferred}
    rest = [p for p in sorted(all_points, key=lambda p: (p["t_io_s"], p["id"]))
            if p["id"] not in seen and
            (p["t_io_s"], p["id"]) >= (point["t_io_s"], point["id"])]
    return preferred + rest


def pick(unique, choices, reason, slot):
    """Pick first unused action vector; record a deterministic duplicate fallback."""
    for index, point in enumerate(choices):
        key = action_key(point)
        if key not in unique:
            unique.add(key)
            return point, reason if index == 0 else (
                f"{reason}; primary duplicated an earlier arm, so selected "
                f"the next worse-ranked unused L0-legal L1 point")
    raise ValueError(f"no unused legal candidate remains for {slot}")


def baseline_slice(points, varying):
    return [p for p in points if all(
        axis == varying or p[axis] == "baseline" for axis in AXES)]


def geometry(point):
    raw = point["predicted_geometry"]
    return {
        "files": raw.get("n_files"),
        "n_rg": raw.get("n_rg"),
        "compressed_bytes": raw.get("compressed_bytes"),
        "n_scan_units": raw.get("n_scan_units"),
    }


def selected_record(candidate_id, point, reason, rank):
    return {
        "candidate_id": candidate_id,
        "source_grid_id": point["id"],
        "selection_reason": reason,
        "actions": point["actions"],
        "l0": {"legal": True, "writer": "pyarrow"},
        "l1": {
            "rank_t_io": rank[point["id"]],
            "t_io_s": point["t_io_s"],
            "ranged_gets": point["ranged_gets"],
            "bytes_gib": point["bytes_gib"],
        },
        "virtual_geometry": geometry(point),
        "axis_labels": {axis: point[axis] for axis in AXES},
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--evidence-dir", required=True,
                    help="directory containing Criteo snapshot/profile/probe")
    ap.add_argument("--sysconst", required=True)
    ap.add_argument("--queries", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--regime", default="measured_cross_cloud")
    ap.add_argument("--seed", type=int, default=20261101)
    args = ap.parse_args()
    if os.environ.get("S3ARR_SCAN_TASK_FLOOR") != "0":
        raise SystemExit(
            "freeze this protocol with S3ARR_SCAN_TASK_FLOOR=0; this matches "
            "the approved Criteo planning grid and its written positive control")

    evidence = Path(args.evidence_dir)
    inputs = {
        "dataset_snapshot": evidence / "dataset_snapshot.json",
        "access_profile": evidence / "access_profile.json",
        "layout_probe": evidence / "layout_probe.json",
        "sysconst": Path(args.sysconst),
        "queries": Path(args.queries),
    }
    missing = [str(path) for path in inputs.values() if not path.is_file()]
    if missing:
        raise SystemExit("missing frozen evidence: " + ", ".join(missing))

    snap = dataset_snapshot.DatasetSnapshot(load_json(inputs["dataset_snapshot"]))
    profile = access_profile.AccessProfile(load_json(inputs["access_profile"]))
    catalog = advisor_catalog.AdvisorCatalog(snap, profile)
    whatif.bind_catalog(catalog)
    vf.bind_probe(load_json(inputs["layout_probe"]))
    sysconst = load_json(inputs["sysconst"])
    if args.regime not in sysconst["regimes"]:
        raise SystemExit(f"regime not in sysconst: {args.regime}")

    table = "criteo"
    if table not in catalog.tables_observed():
        raise SystemExit(f"{table} is not represented by the frozen access profile")
    points, rejected, notes = decode_rerank.enumerate_legal(
        catalog, table, sysconst["regimes"][args.regime],
        sysconst.get("vectored") or {}, vf.probe, set(plan.SEARCHED_AXES),
        "pyarrow")
    if not points:
        raise SystemExit("L0 rejected every candidate")
    ordered = sorted(points, key=lambda p: (p["t_io_s"], p["id"]))
    rank = {point["id"]: index for index, point in enumerate(ordered, 1)}
    baseline = next((p for p in ordered
                     if all(p[axis] == "baseline" for axis in AXES)), None)
    if baseline is None:
        raise SystemExit("the L0-legal grid has no source-baseline point")

    used = set()
    selections = []

    def add(candidate_id, choices, reason):
        point, selected_reason = pick(used, choices, reason, candidate_id)
        selections.append(selected_record(candidate_id, point, selected_reason, rank))

    add("c01-source-baseline", [baseline],
        "source layout / all virtual axes at baseline")
    add("c02-l1-top1-final-joint", ordered,
        "lowest L1 t_io_s among every L0-legal joint candidate")
    order_slice = baseline_slice(points, "column_order")
    file_slice = baseline_slice(points, "file")
    rg_slice = baseline_slice(points, "row_group")
    # Pure codec-only candidates are L0-illegal on this source geometry. Keep
    # the codec+encoding diagnostic legal by allowing its necessary co-actions.
    compression_slice = [p for p in points
                         if p["compression"] == "joint-codec-encoding"]
    add("c03-order-only", with_global_fallback(
        order_slice, min(order_slice,
            key=lambda p: (p["t_io_s"], p["id"])), ordered),
        "L1 winner while only column order may differ from baseline")
    add("c04-file-only", with_global_fallback(
        file_slice, min(file_slice,
            key=lambda p: (p["t_io_s"], p["id"])), ordered),
        "L1 winner while only target file size may differ from baseline")
    add("c05-row-group-only", with_global_fallback(
        rg_slice, min(rg_slice,
            key=lambda p: (p["t_io_s"], p["id"])), ordered),
        "L1 winner while only row-group target may differ from baseline")
    add("c06-joint-codec-encoding-only", with_global_fallback(
        compression_slice, min(compression_slice,
                               key=lambda p: (p["t_io_s"], p["id"])), ordered),
        "best legal L1 point carrying the joint codec+encoding action")

    q75_index = max(0, math.ceil(len(ordered) * .75) - 1)
    add("c07-l1-q75-poor", ordered[q75_index:],
        "first unused non-baseline legal candidate at or worse than L1 75th percentile")
    add("c08-l1-worst", list(reversed(ordered)),
        "worst unused L0-legal candidate by L1 t_io_s")

    if len({action_key({"actions": c["actions"]}) for c in selections}) != 8:
        raise SystemExit("candidate action vectors are not unique")
    for record in selections:
        with_actions = {
            "plan_id": record["candidate_id"],
            "target": {"tables": [table]},
            "writer": "pyarrow",
            "actions": record["actions"],
        }
        plans = Path(args.out) / "plans"
        plans.mkdir(parents=True, exist_ok=True)
        (plans / f"{record['candidate_id']}.json").write_text(
            json.dumps(with_actions, indent=2) + "\n", encoding="utf-8")

    candidate_ids = [record["candidate_id"] for record in selections]
    rng = random.Random(args.seed)
    rounds = []
    for round_number in range(1, 6):
        order = candidate_ids[:]
        rng.shuffle(order)
        rounds.append({"round": round_number, "candidate_order": order})

    output = {
        "schema_version": 1,
        "study": "criteo_l1_rank_fidelity",
        "frozen_before_measurement": True,
        "table": table,
        "writer": "pyarrow",
        "regime": args.regime,
        "scan_task_floor": 0,
        "l1_score": "t_io_s",
        "n_legal": len(points),
        "n_l0_rejected": rejected,
        "selection_policy": (
            "L0/L1 evidence only.  Duplicate single-axis action vectors are "
            "replaced by the next worse-ranked unused L0-legal point; when a "
            "pure single-axis point is L0-illegal, required co-actions remain "
            "visible in axis_labels."),
        "evidence_sha256": {name: sha256(path) for name, path in inputs.items()},
        "notes": notes,
        "candidates": selections,
        "randomization": {"seed": args.seed, "rounds": rounds},
    }
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    (out / "l1_grid.json").write_text(json.dumps({
        "n_legal": len(points), "n_l0_rejected": rejected, "points": ordered,
    }, indent=2) + "\n", encoding="utf-8")
    (out / "protocol.json").write_text(json.dumps(output, indent=2) + "\n",
                                       encoding="utf-8")
    print(f"frozen {len(selections)} candidates from {len(points)} legal points")
    for record in selections:
        print(f"{record['candidate_id']:32} rank={record['l1']['rank_t_io']:3} "
              f"t_io={record['l1']['t_io_s']:8.3f}s {record['source_grid_id']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
