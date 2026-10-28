#!/usr/bin/env python3
"""Phase 3 §8.1: capacity-curve report (RQ2) and the G1 verdict.

Reads a `run_track1_p3.py --phase cap-sf1|cap-sf8` output directory and writes
`capacity_report.json` and `capacity_report.md` next to it. Tables follow
PHASE3_PLAN §5.4: (1) rounds, median, CV, GET, bytes, heap; (2) the capacity
curve with gain versus no cache and versus the reference arm; (3) per-round paired
differences; (4) per-query regression check against the no-cache baseline.
Also checks P-8 (row counts identical across every cell and round) and lists any
failed or missing runs (a heap-infeasible arm is archived, not dropped: P-11).

Wall clock is `runs[0].query_sum_s`, the same quantity `run_track1_matrix`
ranks by. Ranking uses medians only; GETs, bytes, hits, heap and GC explain.

G1 (fixed before the data was seen):
  gap   = median over rounds of (reference wall - best larger arm wall) / median reference wall
  PASS         every paired round positive and gap >= 5%
  FAIL         gap < 2%, or the paired sign flips
  INCONCLUSIVE otherwise (2% <= gap < 5%, same sign): extend to n=5
  A verdict from fewer than 5 rounds is labelled screening.

  python3 tools/track2/phase3_capacity_report.py --out <run dir> \\
      [--reference pol-wtinylfu-av-4g] [--dataset-gib 112.0] \\
      [--append-result docs/adaptive-range-reader/PHASE3_RESULT.md --title "3. ..."]
"""
import argparse
import json
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import run_track1_matrix as m  # noqa: E402
import run_track1_p3 as p3  # noqa: E402

GIB = 1 << 30
BASELINE = "000"
G1_PASS = 0.05
G1_FAIL = 0.02
CV_LIMIT = 0.05
REGRESS_FRAC = 0.15
REGRESS_FLOOR_S = 1.0
FORMAL_N = 5


def cell_capacity_mib(cell_id):
    spec = p3.CATALOG.get(cell_id)
    return 0 if spec is None or not spec.d1 else spec.d1_cache_mib


def load_run(path):
    """One (cell, round) -> dict, or a failure record."""
    if not os.path.isfile(path):
        return {"ok": False, "reason": "missing report.json"}
    try:
        run = m.load(path)["runs"][0]
    except (OSError, ValueError, KeyError, IndexError) as exc:
        return {"ok": False, "reason": f"unreadable report: {exc}"}
    errors = [f"Q{q['query']}: {q['error'][:120]}" for q in run.get("queries") or []
              if q.get("error")]
    t1 = run.get("track1") or {}
    io = run.get("io") or {}
    return {
        "ok": not errors and bool(run.get("queries")),
        "reason": "; ".join(errors[:3]) if errors else None,
        "wall_s": run.get("query_sum_s"),
        "rows": {q["query"]: q.get("rows") for q in run.get("queries") or []},
        "per_query": {q["query"]: q["wall_s"] for q in run.get("queries") or []},
        "gets": io.get("gets"), "remote_bytes": io.get("remote_bytes"),
        "hits": t1.get("cache_hits"), "peak_heap": t1.get("peak_heap_bytes"),
        "gc_ms": t1.get("gc_ms"), "peak_cached": t1.get("peak_cached_bytes"),
        "evicted": t1.get("evicted_bytes"), "ghost_hits": t1.get("d1_ghost_hits"),
    }


def load_cells(out):
    cells = {}
    root = os.path.join(out, "cells")
    for cid in sorted(os.listdir(root)) if os.path.isdir(root) else []:
        rounds = {}
        for name in sorted(os.listdir(os.path.join(root, cid))):
            if name.startswith("run-"):
                rounds[int(name[4:])] = load_run(os.path.join(root, cid, name, "report.json"))
        cells[cid] = rounds
    return cells


def med(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def cv(values):
    if len(values) < 2:
        return None
    mean = statistics.mean(values)
    return statistics.stdev(values) / mean if mean else None


def decide_g1(ref_walls, best_walls):
    """ref_walls / best_walls: {round: wall}. Returns the verdict record."""
    rounds = sorted(set(ref_walls) & set(best_walls))
    diffs = [ref_walls[r] - best_walls[r] for r in rounds]
    base = med([ref_walls[r] for r in rounds])
    out = {"rounds": rounds, "diffs_s": diffs, "n": len(rounds),
           "level": "formal" if len(rounds) >= FORMAL_N else "screening"}
    if len(rounds) < 3 or not base:
        out.update(verdict="INSUFFICIENT", gap=None,
                   why="fewer than 3 paired rounds")
        return out
    gap = statistics.median(diffs) / base
    same_sign = all(d > 0 for d in diffs)
    out["gap"] = gap
    out["all_positive"] = same_sign
    if same_sign and gap >= G1_PASS:
        out.update(verdict="PASS", why="every paired round faster and gap >= 5%")
    elif gap < G1_FAIL or not same_sign:
        out.update(verdict="FAIL", why="gap < 2%" if gap < G1_FAIL else "paired sign flips")
    else:
        out.update(verdict="INCONCLUSIVE", why="2% <= gap < 5%: extend to n=5")
    return out


def build(out, reference, dataset_gib):
    cells = load_cells(out)
    if BASELINE not in cells:
        raise SystemExit(f"no {BASELINE} cell under {out}/cells")
    ok_walls = {cid: {r: v["wall_s"] for r, v in rs.items() if v["ok"]} for cid, rs in cells.items()}
    caps = {cid: cell_capacity_mib(cid) for cid in cells}
    order = sorted(cells, key=lambda c: (caps[c], c))
    if reference is None:
        reference = next((c for c in order if c.endswith("-4g") or c.endswith("-512m")), None)
    if reference not in cells:
        raise SystemExit(f"reference arm {reference!r} not in {sorted(cells)}")

    rows = []
    for cid in order:
        rs = cells[cid]
        good = [v for v in rs.values() if v["ok"]]
        walls = [v["wall_s"] for v in good]
        cap = caps[cid]
        rows.append({
            "cell": cid, "capacity_mib": cap,
            "rho": (cap / 1024 / dataset_gib) if dataset_gib and cap else None,
            "rounds_ok": len(good), "rounds_total": len(rs),
            "walls_s": {r: v["wall_s"] for r, v in sorted(rs.items()) if v["ok"]},
            "median_s": med(walls), "cv": cv(walls),
            "gets": med([v["gets"] for v in good]),
            "remote_gib": (med([v["remote_bytes"] for v in good]) or 0) / GIB if good else None,
            "hits": med([v["hits"] for v in good]),
            "peak_heap_gib": (med([v["peak_heap"] for v in good]) or 0) / GIB if good else None,
            "gc_s": (med([v["gc_ms"] for v in good]) or 0) / 1000 if good else None,
            "peak_cached_gib": (med([v["peak_cached"] for v in good]) or 0) / GIB if good else None,
            "evicted_gib": (med([v["evicted"] for v in good]) or 0) / GIB if good else None,
            "ghost_hits": med([v["ghost_hits"] for v in good]),
            "cv_over_limit": (cv(walls) or 0) > CV_LIMIT,
        })
    by_cell = {r["cell"]: r for r in rows}
    base_med = by_cell[BASELINE]["median_s"]
    ref_med = by_cell[reference]["median_s"]

    curve, prev = [], None
    for r in rows:
        if r["median_s"] is None:
            continue
        curve.append({
            "cell": r["cell"], "capacity_mib": r["capacity_mib"], "rho": r["rho"],
            "median_s": r["median_s"],
            "gain_vs_000": 1 - r["median_s"] / base_med if base_med else None,
            "gain_vs_ref": 1 - r["median_s"] / ref_med if ref_med else None,
            "step_gain": (1 - r["median_s"] / prev) if prev and r["capacity_mib"] else None,
        })
        if r["capacity_mib"]:
            prev = r["median_s"]

    paired = []
    ref_w = ok_walls[reference]
    for cid in order:
        if cid in (reference,):
            continue
        rounds = sorted(set(ref_w) & set(ok_walls[cid]))
        diffs = [ref_w[r] - ok_walls[cid][r] for r in rounds]
        paired.append({"cell": cid, "rounds": rounds, "diffs_s": diffs,
                       "sign_flip": bool(diffs) and not (all(d > 0 for d in diffs)
                                                         or all(d < 0 for d in diffs))})

    base_q = {}
    for v in cells[BASELINE].values():
        if v["ok"]:
            for q, w in v["per_query"].items():
                base_q.setdefault(q, []).append(w)
    base_q = {q: statistics.median(ws) for q, ws in base_q.items()}
    regressions = []
    for cid in order:
        if cid == BASELINE:
            continue
        per_q = {}
        for v in cells[cid].values():
            if v["ok"]:
                for q, w in v["per_query"].items():
                    per_q.setdefault(q, []).append(w)
        for q, ws in sorted(per_q.items()):
            ref = base_q.get(q)
            mq = statistics.median(ws)
            if ref and mq > ref * (1 + REGRESS_FRAC) and mq - ref >= REGRESS_FLOOR_S:
                regressions.append({"cell": cid, "query": q, "median_s": mq,
                                    "baseline_s": ref, "rel": mq / ref - 1})

    row_sets = {}
    for cid, rs in cells.items():
        for r, v in rs.items():
            if v["ok"]:
                for q, n in v["rows"].items():
                    row_sets.setdefault(q, {}).setdefault(n, []).append(f"{cid}/run-{r}")
    row_mismatch = [{"query": q, "row_counts": {str(n): who[:3] + (["..."] if len(who) > 3 else [])
                                                for n, who in cnts.items()}}
                    for q, cnts in sorted(row_sets.items()) if len(cnts) > 1]

    failures = [{"cell": cid, "round": r, "reason": v["reason"]}
                for cid, rs in cells.items() for r, v in sorted(rs.items()) if not v["ok"]]

    larger = [r for r in rows if r["capacity_mib"] > caps[reference] and r["median_s"] is not None]
    g1 = None
    if larger:
        best = min(larger, key=lambda r: (r["median_s"], r["capacity_mib"]))
        g1 = {"reference": reference, "best_larger": best["cell"], **decide_g1(ref_w, ok_walls[best["cell"]])}
    return {"out": out, "reference": reference, "dataset_gib": dataset_gib,
            "table1": rows, "table2_curve": curve, "table3_paired": paired,
            "table4_regressions": regressions, "p8_row_mismatch": row_mismatch,
            "failures": failures, "g1": g1}


def f(v, spec):
    return "-" if v is None else format(v, spec)


def render(rep):
    L = [f"### 容量曲线汇总（自动生成）\n",
         f"- 目录：`{rep['out']}`；参照臂：`{rep['reference']}`；数据量：{rep['dataset_gib']} GiB。",
         "- 排名只看墙钟中位数（`query_sum_s`）；GET、字节、命中、堆、GC 只作解释。\n",
         "**表 1　各轮墙钟、中位数、CV、GET、远端字节、堆**\n",
         "| 臂 | 容量 MiB | ρ | 成功/总轮 | 各轮墙钟 (s) | 中位数 (s) | CV | GET | 远端 GiB | 命中 | 峰值堆 GiB | GC s | 峰值缓存 GiB | 淘汰 GiB |",
         "| --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for r in rep["table1"]:
        walls = " / ".join(f"{w:.0f}" for _, w in sorted(r["walls_s"].items())) or "-"
        flag = " **(CV>5%，P-6)**" if r["cv_over_limit"] else ""
        L.append(f"| {r['cell']} | {r['capacity_mib'] or '-'} | {f(r['rho'] and r['rho'] * 100, '.1f')}% | "
                 f"{r['rounds_ok']}/{r['rounds_total']} | {walls} | {f(r['median_s'], '.1f')} | "
                 f"{f(r['cv'] and r['cv'] * 100, '.2f')}%{flag} | {f(r['gets'], ',.0f')} | "
                 f"{f(r['remote_gib'], '.1f')} | {f(r['hits'], ',.0f')} | {f(r['peak_heap_gib'], '.1f')} | "
                 f"{f(r['gc_s'], '.1f')} | {f(r['peak_cached_gib'], '.2f')} | {f(r['evicted_gib'], '.2f')} |")
    L += ["", "**表 2　容量曲线**（`step_gain` 为相对前一档缓存的收益，档位约翻倍）\n",
          "| 臂 | 容量 MiB | ρ | 中位数 (s) | 相对无缓存 | 相对参照臂 | 相对前一档 |",
          "| --- | ---: | ---: | ---: | ---: | ---: | ---: |"]
    for c in rep["table2_curve"]:
        L.append(f"| {c['cell']} | {c['capacity_mib'] or '-'} | {f(c['rho'] and c['rho'] * 100, '.1f')}% | "
                 f"{c['median_s']:.1f} | {f(c['gain_vs_000'] and c['gain_vs_000'] * 100, '+.1f')}% | "
                 f"{f(c['gain_vs_ref'] and c['gain_vs_ref'] * 100, '+.1f')}% | "
                 f"{f(c['step_gain'] and c['step_gain'] * 100, '+.1f')}% |")
    L += ["", f"**表 3　同轮配对差值**（参照臂 − 该臂，秒；正值表示该臂更快）\n",
          "| 臂 | 轮次 | 差值 (s) | 符号翻转 |", "| --- | --- | --- | --- |"]
    for p in rep["table3_paired"]:
        L.append(f"| {p['cell']} | {','.join(map(str, p['rounds'])) or '-'} | "
                 f"{' / '.join(f'{d:+.1f}' for d in p['diffs_s']) or '-'} | "
                 f"{'是（不得称“显著”，P-7）' if p['sign_flip'] else '否'} |")
    L += ["", "**表 4　逐查询回归检查**（相对无缓存，慢 >15% 且 ≥1 s）\n"]
    if rep["table4_regressions"]:
        L += ["| 臂 | 查询 | 中位数 (s) | 无缓存 (s) | 相对 |", "| --- | ---: | ---: | ---: | ---: |"]
        L += [f"| {r['cell']} | Q{r['query']} | {r['median_s']:.1f} | {r['baseline_s']:.1f} | "
              f"{r['rel'] * 100:+.0f}% |" for r in rep["table4_regressions"]]
    else:
        L.append("没有查询出现该类回归。")
    L += ["", "**P-8　结果正确性（各查询行数在所有臂、所有轮一致）**\n"]
    L.append("通过（只核对行数；仓库没有结果校验和）。" if not rep["p8_row_mismatch"]
             else "**不通过**：" + json.dumps(rep["p8_row_mismatch"], ensure_ascii=False))
    L += ["", "**失败或缺失的运行（P-11 归档，不删除）**\n"]
    if rep["failures"]:
        L += ["| 臂 | 轮 | 原因 |", "| --- | ---: | --- |"]
        L += [f"| {x['cell']} | {x['round']} | {x['reason']} |" for x in rep["failures"]]
    else:
        L.append("无。")
    g = rep["g1"]
    L += ["", "**G1 判定**\n"]
    if not g:
        L.append("没有比参照臂更大的有效臂，无法判定。")
    else:
        L.append(f"- 参照 `{g['reference']}`，较大臂中最快的是 `{g['best_larger']}`；配对轮数 {g['n']}"
                 f"（{'正式' if g['level'] == 'formal' else '筛选，n<5'}）。")
        L.append(f"- 配对差值：{' / '.join(f'{d:+.1f}' for d in g['diffs_s']) or '-'} s；"
                 f"中位差距 {f(g.get('gap') and g['gap'] * 100, '.1f')}%。")
        L.append(f"- 结论：**{g['verdict']}**（{g['why']}）。")
    return "\n".join(L) + "\n"


def make_report(out, reference=None, dataset_gib=None, append_result=None, title=None):
    """Build the report for a grid directory, write capacity_report.{json,md} there and,
    when `append_result` is given, append it to PHASE3_RESULT.md as a `## <title>` section.
    Returns (report dict, markdown)."""
    if append_result and not title:
        raise SystemExit("--append-result needs --title")
    gib = dataset_gib
    meta_path = os.path.join(out, "meta.json")
    if gib is None and os.path.isfile(meta_path):
        size = (m.load(meta_path).get("dataset") or {}).get("bytes")
        gib = round(size / GIB, 2) if size else None
    rep = build(out, reference, gib)
    md = render(rep)
    with open(os.path.join(out, "capacity_report.json"), "w") as fh:
        json.dump(rep, fh, indent=2, default=str)
    with open(os.path.join(out, "capacity_report.md"), "w") as fh:
        fh.write(md)
    if append_result:
        with open(append_result, "a", encoding="utf-8") as fh:
            fh.write(f"\n---\n\n## {title}\n\n{md}")
    return rep, md


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--reference", default=None)
    ap.add_argument("--dataset-gib", type=float, default=None,
                    help="default: dataset bytes in meta.json")
    ap.add_argument("--append-result", default=None, help="PHASE3_RESULT.md to append to")
    ap.add_argument("--title", default=None, help="section heading used with --append-result")
    args = ap.parse_args()

    rep, md = make_report(args.out, args.reference, args.dataset_gib,
                          append_result=args.append_result, title=args.title)
    print(md)
    if args.append_result:
        print(f"appended to {args.append_result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
