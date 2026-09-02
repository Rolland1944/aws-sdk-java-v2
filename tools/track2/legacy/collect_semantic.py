#!/usr/bin/env python3
"""SemanticCollector: turn Spark event logs into per-query scan fragments.

Contract role (TRACK2_M0_CONTRACT.md 3.1/3.2, TRACK2_PLAN.md 2.1): for every SQL
execution, record the time window, the files scanned, the projected columns and
the pushed-down predicates. correlate.py uses the time window to attribute
physical GETs to a query (the MVP runs queries serially, so a window is an exact
assignment; contract 8 r4 defers CommonAuditContext injection to the concurrent
case).

Everything comes from two event kinds in the Spark event log:

  * SparkListenerSQLExecutionStart / End -- give the execution id, its start and
    end wall-clock time, and `physicalPlanDescription`. The plan text carries the
    `Scan parquet` node with Location (files), PushedFilters and ReadSchema.
  * SparkListenerTaskEnd -- Input Metrics give bytes/records read per stage, used
    as a cross-check on how much the scan actually moved.

The plan is parsed from its text rendering, which is the only form available in
the event log. The parse is deliberately shallow: it finds `Scan parquet` blocks
and reads their Location / PushedFilters / ReadSchema lines. If a future Spark
changes the rendering, the scan simply yields no fragment and the coverage gate
in correlate.py drops -- a loud failure, not a silent wrong answer.

Each scan carries `table` (from Location) and `predicates` (PushedFilters parsed
into {column, op, literal}) next to the raw strings. That is the input
predicates_from_runtime.py turns into layout candidates, so the advisor's sort
and partition keys come from what the engine actually pushed down rather than
from a hand-transcribed query catalogue.

Event logs may be plaintext, .zstd, or .lz4 (Spark compresses when
spark.eventLog.compress=true). Works on a local path or an s3:// URI.

Usage:
  python3 tools/track2/collect_semantic.py \
      --eventlog /data/home/haoyueli/track2-data/eventlogs/ \
      --out docs/adaptive-range-reader/results/track2/semantic.json
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone

# A `Scan parquet` block in the physical plan text. The block runs until the next
# node header (a line like "(2) Filter") or the end of the plan.
SCAN_RE = re.compile(r"\(\d+\) Scan parquet\b")
NODE_RE = re.compile(r"^\(\d+\) ", re.MULTILINE)
CALL_RE = re.compile(r"^(\w+)\((.*)\)$", re.S)

# These three fields are bracketed lists whose elements contain the same
# brackets, so they cannot be matched with a non-greedy regex. A
# `PushedFilters: [..., In(l_shipmode, [MAIL,SHIP])]` stops a `\[(.*?)\]`
# pattern at the `]` closing `[MAIL,SHIP`, and the truncated tail then fails to
# parse and is silently dropped -- which is how every IN predicate in TPC-H
# Q12 and Q19 went missing. Scan for the matching close bracket instead.
BRACKETS = {"[": "]", "<": ">"}


def extract_bracketed(text, label, opener="["):
    """Body of `label ... <opener>...<closer>`, matching brackets. None if absent."""
    at = text.find(label)
    if at < 0:
        return None
    start = text.find(opener, at)
    if start < 0:
        return None
    closer = BRACKETS[opener]
    depth = 0
    for i in range(start, len(text)):
        ch = text[i]
        if ch == opener:
            depth += 1
        elif ch == closer:
            depth -= 1
            if depth == 0:
                return text[start + 1:i]
    return None

# Spark's sources.Filter rendering. The value is our canonical op name, which is
# the same vocabulary workload.QUERIES uses by hand, so a runtime-derived
# catalogue and a hand-written one can be compared key for key.
LEAF_OPS = {
    "EqualTo": "eq",
    "EqualNullSafe": "eq",
    "GreaterThan": "gt",
    "GreaterThanOrEqual": "ge",
    "LessThan": "lt",
    "LessThanOrEqual": "le",
    "In": "in",
    "IsNull": "isnull",
    "IsNotNull": "isnotnull",
    "StringStartsWith": "startswith",
    "StringEndsWith": "endswith",
    "StringContains": "contains",
}
BOOL_OPS = {"And", "Or", "Not"}
# Under a Not, the leaf op flips. Flattening `Not(EqualTo(p_brand,Brand#45))`
# to a bare `eq` would tell L1 the scan keeps 1/ndv of the rows when it keeps
# 1 - 1/ndv: on TPC-H Q16 that is a factor of 24. Ranking candidate sort keys
# does not care (the column is equally interesting either way), but the
# selectivity model does, and workload_snapshot.py feeds the latter.
NEGATED_OPS = {
    "eq": "ne", "ne": "eq",
    "gt": "le", "ge": "lt", "lt": "ge", "le": "gt",
    "in": "notin", "notin": "in",
    "isnull": "isnotnull", "isnotnull": "isnull",
    # No inverse worth modelling: L1 scores these as "no filter".
    "startswith": "unknown", "endswith": "unknown", "contains": "unknown",
}
# Ops that a sort key can turn into row-group skipping. Equality also prunes,
# but on a low-NDV column it is a partition candidate instead, so the two are
# kept apart (see predicates_from_runtime.py).
RANGE_OPS = {"gt", "ge", "lt", "le"}


def split_top_level(text, sep=","):
    """Split on `sep` at bracket depth 0.

    PushedFilters is a comma-separated list whose own elements contain commas:
    `[IsNotNull(l_shipdate), GreaterThanOrEqual(l_shipdate,1994-01-01)]`. A
    plain str.split(",") tears that into `GreaterThanOrEqual(l_shipdate` and
    `1994-01-01)`, losing every literal and inventing junk column names.
    """
    out, depth, current = [], 0, ""
    for ch in text:
        if ch in "([":
            depth += 1
        elif ch in ")]":
            depth -= 1
        if ch == sep and depth == 0:
            out.append(current)
            current = ""
        else:
            current += ch
    if current.strip():
        out.append(current)
    return [c.strip() for c in out if c.strip()]


def parse_filter(text, negate=False):
    """One PushedFilters element -> flat list of leaf predicates.

    And/Or/Not nest other filters, so this recurses. The boolean structure is
    flattened to a leaf list because both consumers treat leaves as a
    conjunction: candidate ranking only asks which columns are filtered, and
    L1 multiplies selectivities. `Not` pushes down onto the leaf op (De
    Morgan), which is exact for `Not(leaf)` and approximate for `Not(Or(...))`
    -- neither workload here contains the latter.
    """
    match = CALL_RE.match(text.strip())
    if not match:
        return []
    op, body = match.group(1), match.group(2)
    if op == "Not":
        return parse_filter(body, not negate)
    if op in BOOL_OPS:
        leaves = []
        for part in split_top_level(body):
            leaves.extend(parse_filter(part, negate))
        return leaves
    if op not in LEAF_OPS:
        return []
    args = split_top_level(body)
    if not args:
        return []
    canonical = LEAF_OPS[op]
    if negate:
        canonical = NEGATED_OPS.get(canonical, "unknown")
    return [{
        "column": args[0].strip(),
        "op": canonical,
        "literal": args[1].strip() if len(args) > 1 else None,
        "negated": negate,
    }]


def table_from_files(files):
    """Last path segment shared by the scanned files, used as the table name.

    Both layouts we write are `<root>/<table>/<parquet files>`, and Spark prints
    the directory, not the members. Partitioned tables print `<table>/<col>=<v>`,
    so partition segments are dropped.
    """
    if not files:
        return None
    segments = [s for s in files[0].strip().rstrip("/").split("/") if s]
    while segments and "=" in segments[-1]:
        segments.pop()
    return segments[-1] if segments else None


def _read_eventlog_text(path):
    """Decompress if needed and return the log as text."""
    if path.endswith(".zstd"):
        import zstandard
        return zstandard.ZstdDecompressor().stream_reader(open(path, "rb")).read().decode("utf-8", "replace")
    if path.endswith(".lz4"):
        try:
            import lz4.frame
            return lz4.frame.open(path, "rb").read().decode("utf-8", "replace")
        except ModuleNotFoundError:
            sys.exit("lz4 event log but lz4 not installed; pip install lz4")
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return fh.read()


def _list_eventlogs(fs_path):
    """All event log files under a directory, or the file itself."""
    if os.path.isfile(fs_path):
        return [fs_path]
    out = []
    for root, _dirs, files in os.walk(fs_path):
        for f in files:
            if f.startswith("events") or f.endswith((".zstd", ".lz4", ".json")):
                out.append(os.path.join(root, f))
    return sorted(out)


def _parse_scan_block(block):
    """Extract Location / PushedFilters / ReadSchema from one Scan parquet block."""
    location = extract_bracketed(block, "Location:", "[")
    pushed = extract_bracketed(block, "PushedFilters:", "[")
    schema = extract_bracketed(block, "ReadSchema:", "<")
    files = []
    if location:
        # comma-separated list of paths inside the brackets
        files = [p.strip() for p in location.split(",") if p.strip()]
    pushed_filters = []
    predicates = []
    if pushed:
        pushed_filters = split_top_level(pushed)
        for text in pushed_filters:
            predicates.extend(parse_filter(text))
    read_columns = []
    if schema:
        # struct<a:int,b:string,...> -> top-level column names
        read_columns = _top_level_columns(schema)
    return {
        "table": table_from_files(files),
        "files": files,
        # raw rendering kept alongside the parse so a bad parse stays auditable
        "pushed_filters": pushed_filters,
        "predicates": predicates,
        "read_columns": read_columns,
    }


def _top_level_columns(schema_body):
    """Split a struct body into top-level field names, respecting nested <> and ()."""
    cols, depth, current = [], 0, ""
    for ch in schema_body:
        if ch in "<(":
            depth += 1
        elif ch in ">)":
            depth -= 1
        if ch == "," and depth == 0:
            cols.append(current)
            current = ""
        else:
            current += ch
    if current.strip():
        cols.append(current)
    names = []
    for c in cols:
        name = c.split(":", 1)[0].strip()
        if name:
            names.append(name)
    return names


def parse_eventlog(path):
    """Parse one event log into a list of SQL execution fragments."""
    text = _read_eventlog_text(path)
    executions = {}
    task_bytes = {}  # stage_id -> total bytes read
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        event = e.get("Event")
        if event == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart":
            exid = e.get("executionId")
            executions[exid] = {
                "execution_id": exid,
                "description": e.get("description"),
                "start_ms": e.get("time"),
                "end_ms": None,
                "scans": _parse_plan(e.get("physicalPlanDescription", "")),
            }
        elif event == "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd":
            exid = e.get("executionId")
            if exid in executions:
                executions[exid]["end_ms"] = e.get("time")
        elif event == "SparkListenerTaskEnd":
            stage = e.get("Stage ID")
            metrics = e.get("Task Metrics") or {}
            inp = metrics.get("Input Metrics") or {}
            task_bytes[stage] = task_bytes.get(stage, 0) + (inp.get("Bytes Read") or 0)
    return list(executions.values()), task_bytes


def _parse_plan(plan_text):
    """All `Scan parquet` fragments in a physical plan."""
    scans = []
    # split the plan into node blocks, keep those that are scans
    parts = re.split(r"(?=^\(\d+\) )", plan_text, flags=re.MULTILINE)
    for part in parts:
        if SCAN_RE.search(part.splitlines()[0] if part else ""):
            scans.append(_parse_scan_block(part))
    return scans


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--eventlog", required=True, help="event log file or directory")
    ap.add_argument("--out", default=None, help="output JSON; default stdout summary")
    args = ap.parse_args()

    logs = _list_eventlogs(args.eventlog)
    if not logs:
        sys.exit(f"no event logs under {args.eventlog}")

    all_executions = []
    for path in logs:
        executions, task_bytes = parse_eventlog(path)
        for ex in executions:
            ex["eventlog"] = path
        all_executions.extend(executions)

    all_executions.sort(key=lambda e: (e.get("start_ms") or 0))
    with_scan = [e for e in all_executions if e["scans"]]
    closed = [e for e in all_executions if e.get("end_ms") is not None]

    result = {
        "collected_at": datetime.now(timezone.utc).isoformat(),
        "contract": "docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md 3.1/3.2",
        "eventlog": args.eventlog,
        "sql_executions": len(all_executions),
        "with_scan": len(with_scan),
        "with_time_window": len(closed),
        "executions": all_executions,
    }

    if args.out:
        os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
        with open(args.out, "w") as fh:
            json.dump(result, fh, indent=2)

    print(f"# semantic collect: {args.eventlog}")
    print(f"  event logs        {len(logs)}")
    print(f"  sql executions    {len(all_executions)}")
    print(f"  with scan         {len(with_scan)}")
    print(f"  with time window  {len(closed)}")
    for e in with_scan[:5]:
        nfiles = sum(len(s["files"]) for s in e["scans"])
        ncols = sum(len(s["read_columns"]) for s in e["scans"])
        window = (e.get("end_ms") or 0) - (e.get("start_ms") or 0)
        print(f"    exec {e['execution_id']}: {nfiles} files, {ncols} cols, "
              f"{len(e['scans'])} scans, window {window}ms")
    if args.out:
        print(f"  out               {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
