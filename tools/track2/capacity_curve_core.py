#!/usr/bin/env python3
"""Building blocks for run_capacity_curve.py (Phase 3 §8.2): paths, state, running one
grid in a shim-free environment, failure classification, completeness checks."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

import phase3_capacity_report as cap  # noqa: E402
import phase3_meta as meta  # noqa: E402
import run_track1_p3 as p3  # noqa: E402

REPO = meta.REPO
RESULTS = os.path.join(REPO, "docs/adaptive-range-reader/results/phase3")
RESULT_MD = os.path.join(REPO, "docs/adaptive-range-reader/PHASE3_RESULT.md")
RUN_P3 = os.path.join(HERE, "run_track1_p3.py")

STAGES = ("sf1", "smoke", "screen", "topup")
STAGE_GRID = {"sf1": "sf1", "smoke": "smoke", "screen": "sf8", "topup": "sf8"}
RUNS = {"sf1": 5, "smoke": 1, "screen": 3, "topup": 5}
# minutes per (cell, round), from earlier runs: SF1 ~5, SF8 ~35 (2000-2250 s)
EST_MIN = {"sf1": 5, "smoke": 35, "screen": 35, "topup": 35}
HEAP_GIB = 48
LOG_HEAD = 64 * 1024
LOG_TAIL = 4 * 1024 * 1024

FAILURE_PATTERNS = (
    ("code_state", r"code state changed since"),
    ("preflight", r"PREFLIGHT FAIL"),
    ("disk_guard", r"DISK GUARD"),
    ("enospc", r"No space left on device"),
    ("oom", r"OutOfMemoryError|GC overhead limit exceeded|Java heap space"),
)
HINTS = {
    "code_state": "the code or JAR changed since this grid started (P-14): restore it, or start over with a new TAG",
    "preflight": "pre-flight failed; see the PREFLIGHT FAIL lines in the grid's run.log",
    "disk_guard": "not enough free space on the scratch disk; free space and resume",
    "enospc": "the disk filled up during a run; free space and resume (the failed round is re-run)",
    "oom": "OutOfMemoryError in the driver",
    "other": "see the end of the grid's run.log",
}


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def cid(size):
    return p3.policy_cell(p3.CAP_POLICY, size).id


def paths(tag):
    return {
        "root": os.path.join(RESULTS, f"capacity_curve_aws_{tag}"),
        "sf1": os.path.join(RESULTS, f"clickbench_sf1_capacity_aws_{tag}"),
        "smoke": os.path.join(RESULTS, f"clickbench_sf8_capacity_smoke_aws_{tag}"),
        "sf8": os.path.join(RESULTS, f"clickbench_sf8_capacity_aws_{tag}"),
    }


def sf1_cells():
    return ["000"] + [cid(s) for s in p3.CAP_SF1_SIZES]


def sf8_cells(drop_16g=False):
    return ["000"] + [cid(s) for s in p3.CAP_SF8_SIZES if not (drop_16g and s == "16g")]


class Status:
    """status.json: stage states, recorded facts, run state. Written atomically."""

    def __init__(self, path):
        self.path = path
        self.d = {"stages": {}, "facts": {}, "run": {}}
        if path != os.devnull and os.path.isfile(path):
            with open(path) as fh:
                self.d.update(json.load(fh))

    def save(self):
        if self.path == os.devnull:
            return
        tmp = self.path + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(self.d, fh, indent=2, default=str)
        os.replace(tmp, self.path)

    def stage(self, name):
        return self.d["stages"].get(name, {})

    def set_stage(self, name, **kw):
        self.d["stages"].setdefault(name, {}).update(kw)
        self.save()

    def fact(self, key, default=None):
        return self.d["facts"].get(key, default)

    def set_fact(self, key, value):
        self.d["facts"][key] = value
        self.save()


def read_segment(path, offset):
    """Start and end of what one launch wrote (a full SF8 run logs ~80 MB of progress bars)."""
    with open(path, "rb") as fh:
        fh.seek(0, os.SEEK_END)
        end = fh.tell()
        fh.seek(offset)
        head = fh.read(min(LOG_HEAD, end - offset))
        tail = b""
        if end - offset > LOG_HEAD:
            fh.seek(max(offset + LOG_HEAD, end - LOG_TAIL))
            tail = fh.read()
    return (head + b"\n" + tail).decode("utf-8", "replace").replace("\r", "\n")


def run_grid(argv, out):
    """Run one run_track1_p3.py invocation in a shim-free environment; stdout goes to
    <out>/run.log. Returns (exit code, the part of the log this launch wrote)."""
    os.makedirs(out, exist_ok=True)
    log_path = os.path.join(out, "run.log")
    offset = os.path.getsize(log_path) if os.path.exists(log_path) else 0
    with open(log_path, "ab") as fh:
        fh.write(f"\n##### launch {now()} :: {' '.join(argv[1:])}\n".encode())
        fh.flush()
        rc = subprocess.call(argv, stdout=fh, stderr=subprocess.STDOUT,
                             stdin=subprocess.DEVNULL, env=meta.clean_env())
    text = read_segment(log_path, offset)
    cell = scan_last_cell(log_path, offset)
    return rc, text + (f"\n##### last-slot {cell}\n" if cell else "")


def scan_last_cell(path, offset, chunk=16 << 20):
    """Cell id of the last `=== i/n <cell> run r ===` header this launch wrote. Scans the
    whole segment: a run logs ~80 MB of progress bars, so the header can lie far outside
    the head/tail window that read_segment keeps."""
    pat = re.compile(rb"=== \d+/\d+ (\S+) run \d+")
    last, keep = None, b""
    with open(path, "rb") as fh:
        fh.seek(offset)
        while True:
            block = fh.read(chunk)
            if not block:
                break
            data = (keep + block).replace(b"\r", b"\n")
            for m in pat.finditer(data):
                last = m.group(1).decode()
            keep = data[-256:]
    return last


def last_slot_cell(text):
    m = re.findall(r"^##### last-slot (\S+)$", text or "", re.M)
    return m[-1] if m else None


def classify(text):
    for kind, pattern in FAILURE_PATTERNS:
        if re.search(pattern, text or ""):
            return kind
    return "other"


def grid_complete(out, cells, runs):
    """True when every (cell, round <= runs) has a report without query errors."""
    have = cap.load_cells(out) if os.path.isdir(os.path.join(out, "cells")) else {}
    missing = [(c, r) for c in cells for r in range(1, runs + 1)
               if not (have.get(c, {}).get(r) or {}).get("ok")]
    return not missing, missing


def failing_cells(out):
    have = cap.load_cells(out) if os.path.isdir(os.path.join(out, "cells")) else {}
    return {c for c, rs in have.items() if any(not v["ok"] for v in rs.values())}


def failure_note(rc, text, missing):
    kind = classify(text)
    shown = ", ".join(f"{c}#{r}" for c, r in missing[:6]) + (" ..." if len(missing) > 6 else "")
    return f"exit {rc}; {kind}: {HINTS[kind]}; missing {shown}"


def next_section(path):
    with open(path, encoding="utf-8") as fh:
        nums = [int(m.group(1)) for m in re.finditer(r"^## (\d+)\.", fh.read(), re.M)]
    return max(nums, default=0) + 1


def min_free_during(out, cell, rnd=1):
    f = os.path.join(out, "cells", cell, f"run-{rnd}", "disk.json")
    if os.path.isfile(f):
        with open(f) as fh:
            return json.load(fh).get("min_free_gib_during")
    return None


def archive_heap_infeasible(out, arm, text, where):
    lines = [ln.strip() for ln in (text or "").splitlines()
             if re.search(FAILURE_PATTERNS[-1][1], ln)]
    lines = list(dict.fromkeys(lines))[:12]
    name = "HEAP_INFEASIBLE.md" if where == "smoke" else "HEAP_INFEASIBLE_screen.md"
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, name), "w", encoding="utf-8") as fh:
        fh.write(
            f"# `{arm}` 堆不可行（P-11 归档）\n\n"
            f"- 时间：{now()}\n- 配置：driver 堆 {HEAP_GIB}g，固定容量 16 GiB，算法 `wtinylfu`。\n"
            "- 处理：按项目负责人的决定归档，不换堆大小重跑。\n- 日志里的相关行：\n\n```\n"
            + "\n".join(lines) + "\n```\n")
