#!/usr/bin/env python3
"""EVALUATE side: L0 static checks + L1 analytical cost over a virtual footer.

DB2 Design Advisor compiles each candidate in EVALUATE mode against the
optimizer. We don't have that optimizer, so a candidate is a virtual footer and
the cost is the frozen Reader's request plan:

    t_io = Σ_pattern n_episodes × (RTT × requests + bytes / BW) / K_eff
           K_eff = min(K_busy, n_files_opened, local[N])

r5 simplified this in one structural way worth stating plainly. v1's actions
(sort, partition) changed *which rows* a query read, so the model needed a
selectivity estimate, and it needed an execution residual rescaled by rows to
convert I/O savings into end-to-end time. None of the six v2 actions change the
row set: every candidate reads exactly the same data, just laid out
differently. So the residual is a constant across candidates, ranking by t_io
is ranking by t_e2e, and the whole CDF/prune apparatus is gone.

That simplification has one hole, recorded rather than papered over: a codec
change moves decode CPU, which lands in the residual and is *not* constant.
`advisor_policy.DECODE_MODELLED = False` marks it, and a compression candidate
that wins on predicted bytes alone should be treated as a hypothesis until E-0
measures it.

L0 keeps the checks that still have referents -- writer/reader capability,
readable row-group size, structural fidelity, parallelism floor -- and adds the
three r5 needs: a column order must be a permutation, a codec must be readable,
an encoding must match the physical type.

Usage:
  python3 tools/track2/whatif.py --search --validate \
      --sysconst .../sysconst.json --plans .../plans \
      --dataset-snapshot .../dataset_snapshot.json \
      --access-profile .../access_profile.json \
      --regime measured_cross_cloud
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import advisor_catalog  # noqa: E402
import virtual_footer as vf  # noqa: E402
from advisor_policy import (  # noqa: E402
    FOOTER_BYTES, GUARDRAIL_REGRESSION, HEAD_PER_OPEN, LARGE_TABLE_BYTES,
    MAX_READABLE_RG_BYTES, META_GETS_PER_OPEN, PARALLELISM, VALIDATE_TOL)
from layout_actions import check_l0 as check_actions_l0  # noqa: E402
from layout_actions import render, strip_for_spark  # noqa: E402

# The measured catalog. bind_catalog installs it here and in virtual_footer.
catalog = None


def bind_catalog(cat):
    global catalog
    catalog = cat
    vf.catalog = cat
    return cat


def _primary_table():
    return max(catalog.BASELINE_GEOMETRY,
               key=lambda t: catalog.BASELINE_GEOMETRY[t]["compressed_bytes"])


def physical_types(table):
    """column -> Parquet physical type, for the encoding compatibility check."""
    cols = ((catalog.COLUMN_STATS.get(table) or {}).get("columns") or {})
    return {name: rec.get("physical_type") for name, rec in cols.items()
            if rec.get("physical_type")}


# ------------------------------------------------------------------------- L0

def l0_check(candidate, source_bytes=None, writer="pyarrow"):
    """Contract 5.3 as revised by r5, plus the vectored-timeout readability bound."""
    violations = []
    actions = candidate.get("actions") or []

    for table in catalog.BASELINE_GEOMETRY:
        try:
            rendered = render(actions, table=table)
        except ValueError as exc:
            return False, [str(exc)], None
        if writer == "parquet-mr":
            rendered = strip_for_spark(rendered)
        table_b = (source_bytes if table == _primary_table() and source_bytes
                   else vf.table_bytes(table))
        # Tiny tables cannot satisfy "several row groups"; keep monotonicity only.
        structural = table_b if table_b >= LARGE_TABLE_BYTES else None
        violations.extend(check_actions_l0(
            rendered, structural, schema=catalog.ALL_COLUMNS.get(table),
            physical_types=physical_types(table), writer=writer))

        rg = vf.layout_for(table, candidate)["rg_bytes"]
        if rg and rg > MAX_READABLE_RG_BYTES:
            violations.append(
                f"{table} requested row-group {rg} B exceeds readable bound "
                f"{MAX_READABLE_RG_BYTES} B (parquet vectored wait is hardcoded "
                f"300s; the M2 canary's 35-182 MiB ranges timed out)")

    primary = _primary_table()
    geom = vf.predict_geometry(primary, candidate)
    if geom["n_files"] < 2:
        violations.append(f"{primary} files {geom['n_files']} < 2")
    if geom["n_rg"] < 4:
        violations.append(f"{primary} row groups {geom['n_rg']} < 4")
    if geom["n_files"] > 20000:
        violations.append(f"{primary} files {geom['n_files']} exceed rewrite budget")

    # Parallelism floor: each large table on its own predicted file count.
    for table, base in catalog.BASELINE_GEOMETRY.items():
        if base["compressed_bytes"] < LARGE_TABLE_BYTES:
            continue
        g = vf.predict_geometry(table, candidate)
        floor = min(PARALLELISM, base["files"])
        if g["n_files"] < floor:
            violations.append(
                f"{table} files {g['n_files']} < parallelism floor {floor} "
                f"(baseline {base['files']})")

    return (not violations), violations, geom


# ------------------------------------------------------------------------- L1

def evaluate_pattern(pattern, candidate, regime, vectored):
    """Price one access pattern: one representative episode × its frequency."""
    table = pattern["table"]
    geom = vf.predict_geometry(table, candidate)
    order = vf.column_order_for(table, candidate)
    lay = vf.layout_for(table, candidate)
    min_seek = vectored.get("min_seek_bytes", 131072)
    max_merged = vectored.get("max_merged_bytes", 2097152)

    gets_per_rg, bytes_per_rg = vf.merge_gets(
        table, pattern["columns"], geom, min_seek, max_merged,
        order=order, column_ratios=lay.get("column_ratios"))
    gets_per_rg *= vf.page_split_factor(geom)

    # One episode is one (thread, object) run: it opens one file and walks the
    # row groups in it. Baseline rg-per-episode is measured; a candidate that
    # changes the row-group size changes it proportionally.
    base_geom = catalog.BASELINE_GEOMETRY[table]
    base_rg_per_file = max(1.0, base_geom.get("rg_per_file") or 1.0)
    rg_per_episode = max(1.0, (pattern.get("rg_per_episode") or base_rg_per_file)
                         * geom["rg_per_file"] / base_rg_per_file)

    data_gets = gets_per_rg * rg_per_episode
    data_bytes = bytes_per_rg * rg_per_episode
    meta_gets = META_GETS_PER_OPEN
    meta_bytes = FOOTER_BYTES * META_GETS_PER_OPEN
    heads = HEAD_PER_OPEN

    n_ep = pattern["n_episodes"]
    rtt = regime["rtt_s"]
    bw = regime["bw_bps"]
    k_busy = regime.get("K_busy") or 1.0
    # Episodes of one pattern run concurrently across files and threads, so the
    # ceiling is the file count, not the episode count.
    k = max(1.0, min(k_busy, geom["n_files"], PARALLELISM))
    requests = data_gets + meta_gets + heads
    nbytes = data_bytes + meta_bytes
    t_io = n_ep * (requests * rtt + (nbytes / bw if bw else 0.0)) / k

    return {
        "pattern_id": pattern["pattern_id"],
        "table": table,
        "n_episodes": n_ep,
        "n_columns": len(pattern["columns"]),
        "gets_per_rg": round(gets_per_rg, 3),
        "rg_per_episode": round(rg_per_episode, 2),
        "data_gets": data_gets * n_ep,
        "data_bytes": data_bytes * n_ep,
        "meta_gets": meta_gets * n_ep,
        "meta_bytes": meta_bytes * n_ep,
        "heads": heads * n_ep,
        "k_eff": round(k, 2),
        "t_io_s": t_io,
    }


def evaluate_workload(candidate, patterns, regime, vectored, residual_s=0.0):
    per_pattern = []
    sum_io = 0.0
    sum_gets = sum_bytes = sum_heads = 0.0
    for pattern in patterns:
        if pattern["table"] not in catalog.BASELINE_GEOMETRY:
            continue
        rec = evaluate_pattern(pattern, candidate, regime, vectored)
        per_pattern.append(rec)
        sum_io += rec["t_io_s"]
        sum_gets += rec["data_gets"] + rec["meta_gets"]
        sum_bytes += rec["data_bytes"] + rec["meta_bytes"]
        sum_heads += rec["heads"]
    return {
        "candidate_id": candidate.get("candidate_id") or candidate.get("plan_id"),
        "t_io_s": round(sum_io, 3),
        # No v2 action changes the row set, so the residual is the same constant
        # for every candidate and t_e2e ranks identically to t_io. It is carried
        # anyway so the number printed is comparable with a measured wall clock.
        "t_residual_s": round(residual_s, 3),
        "t_e2e_s": round(sum_io + residual_s, 3),
        "ranged_gets": int(round(sum_gets)),
        "heads": int(round(sum_heads)),
        "bytes": int(round(sum_bytes)),
        "bytes_gib": round(sum_bytes / 2 ** 30, 3),
        "per_pattern": per_pattern,
    }


def pattern_regressions(ev, baseline_per_pattern):
    """Per-pattern predicted t_io vs the same L1 on the baseline."""
    base = {r["pattern_id"]: r for r in baseline_per_pattern}
    out = []
    for rec in ev["per_pattern"]:
        b = base.get(rec["pattern_id"])
        if not b or not b["t_io_s"]:
            continue
        out.append({
            "pattern_id": rec["pattern_id"],
            "t_io_s": round(rec["t_io_s"], 3),
            "baseline_t_io_s": round(b["t_io_s"], 3),
            "regression": round(rec["t_io_s"] / b["t_io_s"] - 1.0, 4),
        })
    return out


def guardrail_regressions(ev, baseline_per_pattern, limit=GUARDRAIL_REGRESSION):
    return [r for r in pattern_regressions(ev, baseline_per_pattern)
            if r["regression"] > limit]


# -------------------------------------------------------------- self-consistency

def observed_io(profile_doc):
    """(ranged GETs, GiB) actually recorded, for the L1 replay gate."""
    shape = profile_doc.get("request_shape") or {}
    reqs = (shape.get("data_requests") or 0) + (shape.get("meta_requests") or 0)
    nbytes = (shape.get("data_bytes") or 0) + (shape.get("meta_bytes") or 0)
    return reqs or None, (nbytes / 2 ** 30 if nbytes else None)


def run_validate(regime, vectored, profile_doc):
    """Replaying the baseline must reproduce the GETs and bytes we recorded."""
    baseline = {"candidate_id": "baseline", "actions": []}
    ev = evaluate_workload(baseline, catalog.PATTERNS, regime, vectored)
    obs_gets, obs_gib = observed_io(profile_doc)
    if obs_gets and obs_gib:
        gets_err = abs(ev["ranged_gets"] - obs_gets) / obs_gets
        gib_err = abs(ev["bytes_gib"] - obs_gib) / obs_gib
        ok = gets_err <= VALIDATE_TOL and gib_err <= VALIDATE_TOL
    else:
        gets_err = gib_err = None
        ok = True
    return {
        "gate": "L1 self-consistency vs the access profile it was built from",
        "predicted_gets": ev["ranged_gets"],
        "observed_gets": obs_gets,
        "gets_rel_error": None if gets_err is None else round(gets_err, 4),
        "predicted_gib": ev["bytes_gib"],
        "observed_gib": None if obs_gib is None else round(obs_gib, 3),
        "gib_rel_error": None if gib_err is None else round(gib_err, 4),
        "tolerance": VALIDATE_TOL,
        "pass": ok,
        "t_io_s": ev["t_io_s"],
        "note": None if obs_gets else "no observed IO in the profile; search still runs",
    }, ev, ok


def load_plans(path):
    """One plan file, or every *.json in a directory."""
    if os.path.isdir(path):
        out = []
        for name in sorted(glob.glob(os.path.join(path, "*.json"))):
            with open(name) as fh:
                doc = json.load(fh)
            out.extend(doc if isinstance(doc, list) else [doc])
        return out
    with open(path) as fh:
        doc = json.load(fh)
    return doc if isinstance(doc, list) else [doc]


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sysconst", required=True)
    ap.add_argument("--plans", default=None,
                    help="plan JSON file or directory of them")
    advisor_catalog.add_arguments(ap)
    ap.add_argument("--regime", default="measured_cross_cloud")
    ap.add_argument("--writer", choices=("pyarrow", "parquet-mr"), default="pyarrow",
                    help="which writer's capability matrix L0 checks against")
    ap.add_argument("--compression-probe", default=None,
                    help="compression_probe.py output; without it a codec change "
                         "is recorded but not priced, because the byte effect "
                         "has not been measured")
    ap.add_argument("--residual-s", type=float, default=0.0,
                    help="measured wall clock minus predicted baseline IO; a "
                         "constant across v2 candidates, carried only so the "
                         "printed t_e2e is comparable with a benchmark run")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--search", action="store_true")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()
    bind_catalog(advisor_catalog.from_args(args))

    with open(args.sysconst) as fh:
        sysc = json.load(fh)
    regime = sysc["regimes"][args.regime]
    vectored = sysc.get("vectored") or {}
    profile_doc = catalog.profile.doc
    if args.compression_probe and os.path.exists(args.compression_probe):
        with open(args.compression_probe) as fh:
            vf.probe = json.load(fh).get("tables")

    out_dir = args.out or os.path.dirname(os.path.abspath(args.access_profile))
    os.makedirs(out_dir, exist_ok=True)

    baseline_ev = None
    if args.validate or args.search:
        vreport, baseline_ev, vok = run_validate(regime, vectored, profile_doc)
        with open(os.path.join(out_dir, "validate.json"), "w") as fh:
            json.dump(vreport, fh, indent=2)
        ge, be = vreport.get("gets_rel_error"), vreport.get("gib_rel_error")
        print("# validate")
        print(f"  gets  pred={vreport['predicted_gets']} "
              f"observed={vreport['observed_gets']} "
              f"err={ge if ge is None else round(ge * 100, 1)}")
        print(f"  GiB   pred={vreport['predicted_gib']} "
              f"observed={vreport['observed_gib']} "
              f"err={be if be is None else round(be * 100, 1)}")
        print(f"  gate  {'PASS' if vok else 'FAIL'}  (<= {VALIDATE_TOL * 100:.0f}%)")
        if args.validate and not args.search:
            return 0 if vok else 1

    if not args.search:
        return 0
    if not args.plans:
        raise SystemExit("--search needs --plans")

    plans = load_plans(args.plans)
    baseline_cand = {"candidate_id": "baseline", "actions": []}
    baseline_pred = evaluate_workload(baseline_cand, catalog.PATTERNS, regime,
                                      vectored, args.residual_s)

    ranked = []
    for plan in plans:
        pid = plan.get("plan_id") or plan.get("candidate_id") or "unnamed"
        ok, viol, _geom = l0_check(plan, writer=args.writer)
        if not ok:
            ranked.append({"candidate_id": pid, "l0_ok": False,
                           "l0_violations": viol, "t_io_s": None})
            continue
        ev = evaluate_workload(plan, catalog.PATTERNS, regime, vectored,
                               args.residual_s)
        regs = pattern_regressions(ev, baseline_pred["per_pattern"])
        bad = [r for r in regs if r["regression"] > GUARDRAIL_REGRESSION]
        ranked.append({
            "candidate_id": pid,
            "l0_ok": True,
            "guardrail_ok": not bad,
            "guardrail_regressions": bad,
            "max_regression": round(max((r["regression"] for r in regs), default=0.0), 4),
            "t_io_s": ev["t_io_s"],
            "t_e2e_s": ev["t_e2e_s"],
            "ranged_gets": ev["ranged_gets"],
            "bytes_gib": ev["bytes_gib"],
            "improve_frac": (round(1.0 - ev["t_io_s"] / baseline_pred["t_io_s"], 4)
                             if baseline_pred["t_io_s"] else None),
            "plan": plan,
        })

    legal = [r for r in ranked if r["l0_ok"] and r.get("guardrail_ok")]
    legal.sort(key=lambda r: r["t_io_s"])
    l0_legal = [r for r in ranked if r["l0_ok"]]
    l0_legal.sort(key=lambda r: r["t_io_s"])

    report = {
        "searched_at": datetime.now(timezone.utc).isoformat(),
        "contract": "TRACK2_M0_CONTRACT.md r5",
        "regime": args.regime,
        "writer": args.writer,
        "n_plans": len(plans),
        "n_l0_fail": len(ranked) - len(l0_legal),
        "n_legal": len(legal),
        "baseline_t_io_s": baseline_pred["t_io_s"],
        "baseline_gets": baseline_pred["ranged_gets"],
        "baseline_gib": baseline_pred["bytes_gib"],
        "provenance": catalog.provenance(),
        "decode_modelled": False,
        "note": ("no v2 action changes the row set, so the execution residual is "
                 "constant across candidates and t_io ranks identically to "
                 "t_e2e. The exception is decode CPU under a codec change, "
                 "which L1 does not model."),
        "ranked": [{k: v for k, v in r.items() if k != "plan"} for r in l0_legal],
        "rejected": [r for r in ranked if not r["l0_ok"]],
    }
    path = os.path.join(out_dir, f"whatif_{args.regime}.json")
    with open(path, "w") as fh:
        json.dump(report, fh, indent=2)

    print("# search")
    print(f"  baseline  t_io={baseline_pred['t_io_s']:.1f}s  "
          f"gets={baseline_pred['ranged_gets']}  {baseline_pred['bytes_gib']:.2f}GiB")
    print(f"  legal     {len(legal)} / {len(plans)} "
          f"(L0 fail {report['n_l0_fail']})")
    for r in legal[:5]:
        print(f"    {r['t_io_s']:8.1f}s  {(r['improve_frac'] or 0) * 100:+5.1f}%  "
              f"{r['candidate_id']}")
    for r in ranked:
        if not r["l0_ok"]:
            print(f"    REJECTED {r['candidate_id']}: {r['l0_violations'][0]}")
    print(f"  wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
