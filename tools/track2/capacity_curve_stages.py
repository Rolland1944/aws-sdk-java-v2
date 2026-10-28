#!/usr/bin/env python3
"""The four stages of run_capacity_curve.py (Phase 3 §8.2). Each stage returns
(ok, note); a failed stage stops the driver, and the same --tag resumes it."""
from __future__ import annotations

import sys

import capacity_curve_core as core
from capacity_curve_core import (HEAP_GIB, RUNS, cap, cid, classify, failing_cells,
                                 failure_note, grid_complete, log, min_free_during,
                                 next_section, sf1_cells, sf8_cells)


class Ctx:
    def __init__(self, args, P, st, runner=None):
        self.args, self.P, self.st = args, P, st
        self.runner = runner or core.run_grid

    def argv(self, phase, runs, out, cells=None):
        a = [sys.executable, core.RUN_P3, "--phase", phase, "--runs", str(runs), "--out", out]
        if cells:
            a += ["--cells", ",".join(cells)]
        if self.args.min_free_gib is not None:
            a += ["--min-free-gib", str(self.args.min_free_gib)]
        return a

    def append(self, key, title_tail, writer):
        """Append one section to PHASE3_RESULT.md, at most once per `key`."""
        done = self.st.fact("appended", {})
        if key in done:
            return done[key]
        title = f"{next_section(core.RESULT_MD)}. {title_tail}"
        writer(title)
        done[key] = title
        self.st.set_fact("appended", done)
        log(f"appended to PHASE3_RESULT.md: {title}")
        return title

    def report(self, key, out, reference, title_tail):
        holder = {}

        def write(title):
            holder["rep"], _ = cap.make_report(out, reference, None,
                                               append_result=core.RESULT_MD, title=title)
        try:
            self.append(key, title_tail, write)
        except SystemExit as exc:
            log(f"report for {key} failed: {exc}")
            return None
        return holder.get("rep")

    def text(self, key, title_tail, body):
        def write(title):
            with open(core.RESULT_MD, "a", encoding="utf-8") as fh:
                fh.write(f"\n---\n\n## {title}\n\n{body}")
        self.append(key, title_tail, write)


def stage_sf1(ctx):
    out, runs = ctx.P["sf1"], RUNS["sf1"]
    rc, text = ctx.runner(ctx.argv("cap-sf1", runs, out), out)
    done, missing = grid_complete(out, sf1_cells(), runs)
    if not done:
        return False, failure_note(rc, text, missing)
    ctx.report("sf1", out, cid("512m"),
               f"SF1 容量曲线（`wtinylfu`，driver {HEAP_GIB}g，n={runs}，{ctx.args.tag}）")
    return True, f"{len(sf1_cells())} arms x {runs} rounds"


def stage_smoke(ctx):
    out, arm = ctx.P["smoke"], cid("16g")
    rc, text = ctx.runner(ctx.argv("cap-sf8-smoke", 1, out), out)
    done, missing = grid_complete(out, [arm], 1)
    title = f"SF8 容量曲线：16 GiB 臂堆可行性冒烟（n=1，不报告百分比，{ctx.args.tag}）"
    head = f"- 臂 `{arm}`，SF8 原始布局，driver 堆 {HEAP_GIB}g，n=1（冒烟）。\n"
    if done:
        v = cap.load_run(f"{out}/cells/{arm}/run-1/report.json")
        peak, gc, wall = (v["peak_heap"] or 0) / cap.GIB, (v["gc_ms"] or 0) / 1000, v["wall_s"]
        tight = peak >= 0.9 * HEAP_GIB or (bool(wall) and gc / wall >= 0.05)
        facts = {"feasible": True, "heap_tight": tight, "peak_heap_gib": round(peak, 1),
                 "gc_s": round(gc, 1), "wall_s": wall, "min_free_gib": min_free_during(out, arm)}
        ctx.st.set_fact("heap_infeasible_16g", False)
        ctx.st.set_fact("smoke", facts)
        warn = ("**堆余量很小（峰值堆 ≥ 90% 或 GC ≥ 5% 墙钟），GC 抖动可能影响 16g 臂的墙钟；"
                "是否改用 54g 重跑由项目负责人决定，本脚本不自动处理。**") if tight else "堆余量可接受。"
        ctx.text("smoke", title, head + (
            f"- 结果：**放得下**，43 条查询均无错误。\n"
            f"- 峰值堆 {peak:.1f} GiB（占 {HEAP_GIB}g 的 {peak / HEAP_GIB * 100:.0f}%），GC 总时间 {gc:.1f} s，"
            f"墙钟 {wall:.0f} s（单次原始数，不排名），GET {v['gets']:,}，"
            f"运行中根盘最低剩余 {facts['min_free_gib']} GiB。\n- {warn}\n"))
        return True, f"16g fits: peak heap {peak:.1f} GiB, gc {gc:.1f} s" + ("; HEAP TIGHT" if tight else "")
    if classify(text) == "oom":
        core.archive_heap_infeasible(out, arm, text, "smoke")
        ctx.st.set_fact("heap_infeasible_16g", True)
        ctx.st.set_fact("smoke", {"feasible": False})
        ctx.text("smoke", title, head + (
            "- 结果：**堆不可行**（OutOfMemory）。按项目负责人的决定归档为“堆不可行”（P-11），"
            "不换堆大小重跑；SF8 筛选阶段去掉 16g 臂。证据见该目录的 `HEAP_INFEASIBLE.md`。\n"
            "- 含义：16 GiB 内存缓存在 48g 堆里放不下，这本身支持“内存放不下，需要别的介质”。\n"))
        return True, "16g heap-infeasible (archived; dropped from the screen)"
    return False, failure_note(rc, text, missing)


def stage_screen(ctx):
    out, runs = ctx.P["sf8"], RUNS["screen"]
    drop = bool(ctx.st.fact("heap_infeasible_16g", False))
    for attempt in (1, 2):
        cells = sf8_cells(drop)
        rc, text = ctx.runner(ctx.argv("cap-sf8", runs, out, cells), out)
        done, missing = grid_complete(out, cells, runs)
        if done:
            break
        bad = failing_cells(out)
        only_16g = core.last_slot_cell(text) == cid("16g") or (bool(bad) and bad <= {cid("16g")})
        if attempt == 1 and not drop and classify(text) == "oom" and only_16g:
            core.archive_heap_infeasible(out, cid("16g"), text, "screen")
            ctx.st.set_fact("heap_infeasible_16g", True)
            drop = True
            log(f"{cid('16g')} died of OutOfMemory in the screen: archived, continuing without it")
            continue
        return False, failure_note(rc, text, missing)
    rep = ctx.report("screen", out, cid("4g"),
                     f"SF8 容量曲线筛选（`wtinylfu`，driver {HEAP_GIB}g，n={runs}，{ctx.args.tag}）")
    if rep and rep.get("g1"):
        ctx.st.set_fact("g1_screen", {k: rep["g1"].get(k) for k in
                                     ("reference", "best_larger", "verdict", "gap", "n", "level", "why")})
    return True, f"{len(cells)} arms x {runs} rounds" + ("; 16g dropped" if drop else "")


def stage_topup(ctx):
    out, runs, ref = ctx.P["sf8"], RUNS["topup"], cid("4g")
    try:
        rep, _ = cap.make_report(out, ref, None)
    except SystemExit as exc:
        return False, f"cannot pick the key arm: {exc}"
    g1 = rep.get("g1")
    if not g1 or not g1.get("best_larger"):
        return True, "no valid arm larger than the reference; nothing to top up"
    best = g1["best_larger"]
    cells = ["000", ref, best]
    ctx.st.set_fact("topup_cells", cells)
    done, missing = grid_complete(out, cells, runs)
    if not done:
        rc, text = ctx.runner(ctx.argv("cap-sf8", runs, out, cells), out)
        done, missing = grid_complete(out, cells, runs)
        if not done:
            return False, failure_note(rc, text, missing)
    rep = ctx.report("topup", out, ref,
                     f"SF8 容量曲线补轮至 n={runs}（`wtinylfu`，driver {HEAP_GIB}g，关键臂 `{best}`，{ctx.args.tag}）")
    if rep and rep.get("g1"):
        ctx.st.set_fact("g1_final", {k: rep["g1"].get(k) for k in
                                     ("reference", "best_larger", "verdict", "gap", "n", "level", "why")})
        high = [r["cell"] for r in rep["table1"] if r["cell"] in cells and r["cv_over_limit"]]
        if high:
            ctx.st.set_fact("cv_over_5pct", high)
            log(f"P-6: CV > 5% in {high}: extend those arms to 7 rounds before quoting a percentage")
    return True, f"{', '.join(cells)} up to n={runs}"


STAGE_FN = {"sf1": stage_sf1, "smoke": stage_smoke, "screen": stage_screen, "topup": stage_topup}
