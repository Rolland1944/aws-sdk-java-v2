#!/usr/bin/env python3
"""Phase 3 §8.2: capacity curve (RQ2), unattended driver.

Runs four stages in order, each resumable, and appends the results to PHASE3_RESULT.md
as they finish. Algorithm `wtinylfu`, fixed capacity, admit.bytes=0, doorkeeper off,
driver 48g for every arm (PHASE3_PLAN.md §0.1, §8.2, P-12, P-13).

  sf1     cap-sf1       8 arms x 5 rounds; capacities mirror the SF8 working-set ratios.
  smoke   cap-sf8-smoke 16 GiB arm only, n=1: does a 16 GiB cache fit in the 48g heap?
                        An OutOfMemory is archived as "heap infeasible" (P-11) and is
                        NOT retried with a different heap.
  screen  cap-sf8       6 arms x 3 rounds. The 16g arm is dropped if the smoke showed it
                        infeasible, or if it dies of OutOfMemory during the screen.
  topup   cap-sf8       000, the 4g reference and the faster of 8g/16g, up to n=5.

Step 0 of the original plan (trace replay) was cancelled and is not part of this run.

Normally use the launcher, which detaches the process and strips the IDE's safe-delete shim:

  TAG=20261016 tools/track2/run_capacity_curve.sh start
  tools/track2/run_capacity_curve.sh status | tail | stop | dry-run

Directly (the interpreter must be .venv/bin/python):

  .venv/bin/python tools/track2/run_capacity_curve.py --tag 20261016 [--stages sf1,smoke]
  .venv/bin/python tools/track2/run_capacity_curve.py --tag 20261016 --status | --dry-run

Resume with the same command and the same --tag: stages marked done in status.json are
skipped, a half-finished grid continues at the next (cell, round). P-14: the JAR and the
code state (git HEAD, code diff) must not change between the first start and a resume;
if they did, start over with a new --tag.

G1 is computed by phase3_capacity_report (fixed before the data: PASS needs every paired
round faster and a median gap >= 5%; FAIL below 2% or on a sign flip). This driver does
not decide G1; the verdict goes to status.json and PHASE3_RESULT.md.
"""
from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import signal
import subprocess
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import capacity_curve_core as core  # noqa: E402
from capacity_curve_core import (EST_MIN, HEAP_GIB, RUNS, STAGE_GRID, STAGES, cid, log, now,  # noqa: E402
                                 paths, sf1_cells, sf8_cells)
from capacity_curve_stages import STAGE_FN, Ctx  # noqa: E402
import phase3_meta as meta  # noqa: E402
import s3a_session  # noqa: E402


def plan_lines(P):
    rows = [
        ("sf1", f"cap-sf1, {len(sf1_cells())} arms x {RUNS['sf1']}", len(sf1_cells()) * RUNS["sf1"], P["sf1"]),
        ("smoke", "cap-sf8-smoke, 16g only x 1", 1, P["smoke"]),
        ("screen", f"cap-sf8, {len(sf8_cells())} arms x {RUNS['screen']} (16g dropped if infeasible)",
         len(sf8_cells()) * RUNS["screen"], P["sf8"]),
        ("topup", "cap-sf8, 000 + 4g + best of 8g/16g, rounds 4-5", 3 * (RUNS["topup"] - RUNS["screen"]), P["sf8"]),
    ]
    total, out = 0, []
    for stage, what, n, d in rows:
        mins = n * EST_MIN[stage]
        total += mins
        out.append(f"  {stage:7s} {what:68s} {n:3d} runs x ~{EST_MIN[stage]:2d} min = {mins / 60:5.1f} h"
                   f"  -> {os.path.relpath(d, core.REPO)}")
    out.append(f"  total   ~{total / 60:.1f} h, one process (every run needs all 16 cores)")
    return out


def dry_run(args, P):
    print(f"# capacity curve  tag={args.tag}  stages={','.join(args.stages)}  (dry run: nothing is created)")
    print(f"# policy={core.p3.CAP_POLICY}  driver={HEAP_GIB}g")
    print("\n".join(plan_lines(P)))
    ctx = Ctx(args, P, core.Status(os.devnull))
    pending = "<best of 8g/16g, chosen after the screen>"
    for stage in args.stages:
        out = P[STAGE_GRID[stage]]
        argv = {"sf1": lambda: ctx.argv("cap-sf1", RUNS["sf1"], out),
                "smoke": lambda: ctx.argv("cap-sf8-smoke", 1, out),
                "screen": lambda: ctx.argv("cap-sf8", RUNS["screen"], out, sf8_cells()),
                "topup": lambda: ctx.argv("cap-sf8", RUNS["topup"], out, ["000", cid("4g"), pending])}[stage]()
        print(f"\n[{stage}]\n  " + " ".join(argv))
    problems = meta.preflight()
    print("\n# pre-flight:", "OK" if not problems else "; ".join(problems))
    print("# IDE shim in this environment:",
          "yes (the launcher and the driver strip it)" if meta.needs_clean_env() else "no")
    scratch = s3a_session.spark_scratch()
    ok, free = meta.disk_guard(scratch, 50.0)
    print(f"# scratch {scratch}: {free:.1f} GiB free ({'ok' if ok else 'BELOW the 50 GiB guard'})")
    return 0


def alive(pid):
    try:
        os.kill(int(pid), 0)
        return True
    except (OSError, ValueError, TypeError):
        return False


def show_status(P):
    path = os.path.join(P["root"], "status.json")
    if not os.path.isfile(path):
        print(f"no status at {os.path.relpath(path, core.REPO)}: not started with this tag")
        return 1
    st = core.Status(path)
    pidf = os.path.join(P["root"], "capacity_curve.pid")
    pid = open(pidf).read().strip() if os.path.isfile(pidf) else None
    print(f"process: {'RUNNING pid ' + pid if pid and alive(pid) else 'not running'}"
          f"   run state: {st.d.get('run', {}).get('state', '-')}")
    for s in STAGES:
        d = st.stage(s)
        print(f"  {s:7s} {d.get('state', 'pending'):9s} {d.get('started', ''):21s} "
              f"{d.get('finished', ''):21s} {d.get('note', '')}")
    for k in ("smoke", "heap_infeasible_16g", "g1_screen", "g1_final", "cv_over_5pct", "topup_cells"):
        if st.fact(k) is not None:
            print(f"  {k}: {st.fact(k)}")
    running = next((s for s in STAGES if st.stage(s).get("state") == "running"), None)
    if running:
        out = P[STAGE_GRID[running]]
        lg = os.path.join(out, "run.log")
        if os.path.isfile(lg):
            with open(lg, "rb") as fh:
                fh.seek(0, os.SEEK_END)
                fh.seek(max(0, fh.tell() - 600_000))
                tail = fh.read().decode("utf-8", "replace").replace("\r", "\n")
            slots = re.findall(r"^=== (\d+/\d+ \S+ run \d+)", tail, re.M)
            qs = re.findall(r"^  run \d  Q(\d+)\s+([\d.]+)s\s+(\w+)", tail, re.M)
            reports = subprocess.run(["find", os.path.join(out, "cells"), "-name", "report.json"],
                                     capture_output=True, text=True).stdout.split()
            last = f"Q{qs[-1][0]} {qs[-1][1]}s {qs[-1][2]}" if qs else "-"
            print(f"  now: {slots[-1] if slots else '-'}   last query: {last}   reports so far: {len(reports)}")
    free = meta.free_gib(s3a_session.spark_scratch())
    print(f"  scratch disk free: {free:.1f} GiB")
    return 0


def other_benchmarks():
    out = subprocess.run(["pgrep", "-f", r"run_track1_p3\.py|run_benchmark\.py"],
                         capture_output=True, text=True).stdout.split()
    mine = {os.getpid(), os.getppid()}
    return [int(p) for p in out if int(p) not in mine]


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tag", default=datetime.now().strftime("%Y%m%d"),
                    help="names every output directory; reuse it to resume (default: today)")
    ap.add_argument("--stages", default=",".join(STAGES),
                    help=f"comma-separated subset of {','.join(STAGES)}; always run in that order")
    ap.add_argument("--force", default="", help="comma-separated stages to run again even if done")
    ap.add_argument("--min-free-gib", type=float, default=None,
                    help="forwarded to run_track1_p3.py (its default is 50)")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and commands; start nothing")
    ap.add_argument("--status", action="store_true", help="show the state of a running or finished tag")
    args = ap.parse_args(argv)
    wanted = [s.strip() for s in args.stages.split(",") if s.strip()]
    bad = [s for s in wanted if s not in STAGES]
    if bad:
        ap.error(f"unknown stages {bad}; known {list(STAGES)}")
    args.stages = [s for s in STAGES if s in wanted]
    args.force = [s.strip() for s in args.force.split(",") if s.strip()]
    return args


def run_stages(args, ctx, st):
    for stage in args.stages:
        if st.stage(stage).get("state") == "done" and stage not in args.force:
            log(f"[{stage}] already done, skipping ({st.stage(stage).get('note', '')})")
            continue
        st.set_stage(stage, state="running", started=now(), finished="", note="")
        log(f"[{stage}] start -> {os.path.relpath(ctx.P[STAGE_GRID[stage]], core.REPO)}")
        try:
            ok, note = STAGE_FN[stage](ctx)
        except Exception as exc:  # keep the evidence, then stop
            ok, note = False, f"{type(exc).__name__}: {exc}"
        st.set_stage(stage, state="done" if ok else "failed", finished=now(), note=note)
        log(f"[{stage}] {'done' if ok else 'FAILED'}: {note}")
        if not ok:
            st.d["run"].update(state="failed", at=now())
            st.save()
            log("stopped. Fix the cause and start again with the same --tag to resume.")
            return 1
    st.d["run"].update(state="finished", at=now())
    st.save()
    log("finished. G1: " + json.dumps(st.fact("g1_final") or st.fact("g1_screen") or "n/a",
                                      ensure_ascii=False))
    log("PHASE3_RESULT.md has the new sections; add them to its index (§0) by hand.")
    return 0


def main(argv=None):
    args = parse_args(argv)
    P = paths(args.tag)
    if args.status:
        return show_status(P)
    if args.dry_run:
        return dry_run(args, P)

    meta.reexec_clean()  # same PID, environment without the IDE safe-delete shim
    os.makedirs(P["root"], exist_ok=True)
    lock = open(os.path.join(P["root"], "lock"), "w")
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        print(f"another capacity-curve driver holds {P['root']}/lock; not starting")
        return 3
    with open(os.path.join(P["root"], "capacity_curve.pid"), "w") as fh:
        fh.write(str(os.getpid()))

    st = core.Status(os.path.join(P["root"], "status.json"))
    st.d["run"] = {"state": "starting", "tag": args.tag, "started": now(), "pid": os.getpid(),
                   "stages": args.stages, "argv": sys.argv}
    st.save()

    def interrupted(signum, _frame):
        st.d["run"].update(state="interrupted", signal=signum, at=now())
        st.save()
        log(f"signal {signum}: stopping (resume with the same --tag)")
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)

    log(f"capacity curve  tag={args.tag}  stages={','.join(args.stages)}  python={sys.executable}")
    print("\n".join(plan_lines(P)), flush=True)
    problems = meta.preflight()
    busy = other_benchmarks()
    if busy:
        problems.append(f"another benchmark process is running (pids {busy}); two runs would share the 16 cores")
    ok, free = meta.disk_guard(s3a_session.spark_scratch(), args.min_free_gib or 50.0)
    if not ok:
        problems.append(f"only {free:.1f} GiB free on the scratch disk")
    if problems:
        for p in problems:
            log(f"PREFLIGHT FAIL: {p}")
        st.d["run"].update(state="preflight_failed", problems=problems, at=now())
        st.save()
        return 3
    git = meta.git_info()
    log(f"git HEAD {str(git['git_head'])[:10]}; {len(git['git_untracked_code_files'])} untracked code files, "
        f"diff: {git['git_diff_shortstat'] or 'none'}. Do not commit or rebuild while this runs (P-14).")
    st.d["run"]["state"] = "running"
    st.save()
    return run_stages(args, Ctx(args, P, st), st)


if __name__ == "__main__":
    sys.exit(main())
