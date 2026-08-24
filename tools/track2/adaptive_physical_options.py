#!/usr/bin/env python3
"""Physical action space derived from measured geometry, not a fixed grid.

`FILE_SIZE_GRID = {128MB, 256MB, 512MB, 1GB}` and `RG_SIZE_GRID` were the last
hand-written parts of the action space, and they were hand-written in the wrong
units. Nothing in the cost model cares about "512 MB". What the model reacts to
is the *count*: how many files a scan opens (parallelism, and one RTT-bound
open apiece) and how many row groups a predicate can skip. Bytes are just how
Spark is told to produce that count.

Writing the grid in bytes has two consequences. It does not transfer -- 128 MB
is 170 files of TPC-H lineitem, 110 of ClickBench hits, and 1 file of a 100 MB
table, so the same grid means something different everywhere and something
useless in the third case. And it hides the constraint that actually binds:
L0's floor is `n_files >= min(parallelism, baseline_files)`, a statement about
counts, so a byte grid can only satisfy it by accident and is then silently
filtered afterwards.

So the ladder is built in count space and converted to bytes at the end:

    n_files in {floor, 2*floor, 4*floor, ...} capped at the baseline count
    floor  = min(parallelism, baseline_files)
    bytes  = compressed_bytes / n_files

The baseline count is the cap because more files than the engine already chose
costs opens without buying pruning; pruning granularity is what sort and
partition are for. The floor is L0's, so every generated option is legal by
construction instead of legal by luck.

Row groups are the same argument one level down: the ladder is row groups per
file, bounded below by the point where per-chunk overhead beats skipping and
above by the vectored-read timeout that the M2 canary hit.
"""

from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import advisor_policy as policy  # noqa: E402

# Below this a row group is mostly page headers and dictionary, and the
# per-chunk request overhead beats whatever extra skipping it buys.
MIN_USEFUL_RG_BYTES = 8 * 1024 * 1024
# Row groups per file to consider. 1 is "one row group per file", which makes
# row-group skipping and file skipping the same thing.
RG_PER_FILE_LADDER = (1, 2, 4, 8)


def _uncompressed_bytes(geom):
    """Total uncompressed bytes, which is what a requested RG size is measured in.

    Parquet's `parquet.block.size` bounds the *uncompressed* buffer, so the
    snapshot's mean row-group `total_byte_size` times the row-group count is
    the quantity that divides by a requested size to give a count.
    """
    rg_bytes = geom.get("rg_bytes")
    n_rg = geom.get("n_rg") or (geom["files"] * (geom.get("rg_per_file") or 1))
    if not rg_bytes:
        return None
    return rg_bytes * n_rg


def file_count_ladder(geom, parallelism=policy.PARALLELISM):
    """Legal file counts for one table, coarsest first, baseline last."""
    baseline = geom["files"]
    floor = max(1, min(parallelism, baseline))
    counts, n = [], floor
    while n < baseline:
        counts.append(n)
        n *= 2
    counts.append(baseline)
    return counts


def file_options(table, snapshot, parallelism=policy.PARALLELISM,
                 large_table_bytes=policy.LARGE_TABLE_BYTES):
    """[(label, target-file-size bytes or None)] for one table.

    None is the baseline: emit no `write.target-file-size-bytes` action at all,
    so the engine keeps doing whatever it already does. Tables under
    `large_table_bytes` get only that -- rewriting them cannot repay the
    rewrite, and E8 showed a global file size chosen for a 21 GiB table
    starving a 4 GiB one.
    """
    geom = snapshot.BASELINE_GEOMETRY[table]
    options = [("baseline", None)]
    if geom["compressed_bytes"] < large_table_bytes:
        return options
    for n_files in file_count_ladder(geom, parallelism):
        if n_files >= geom["files"]:
            continue
        options.append((f"{n_files}f", int(geom["compressed_bytes"] // n_files)))
    return options


def rg_options(table, snapshot, max_rg_bytes=policy.MAX_READABLE_RG_BYTES,
               large_table_bytes=policy.LARGE_TABLE_BYTES):
    """[(label, requested row-group bytes or None)] for one table."""
    geom = snapshot.BASELINE_GEOMETRY[table]
    options = [("baseline", None)]
    if geom["compressed_bytes"] < large_table_bytes:
        return options
    total = _uncompressed_bytes(geom)
    if not total:
        return options
    per_file = total / max(geom["files"], 1)
    for k in RG_PER_FILE_LADDER:
        nbytes = int(per_file / k)
        if nbytes > max_rg_bytes or nbytes < MIN_USEFUL_RG_BYTES:
            continue
        options.append((f"{k}rg", nbytes))
    return options


def describe(snapshot, parallelism=policy.PARALLELISM):
    """Per-table action space, for the analyze report and for eyeballing."""
    out = {}
    for table in sorted(snapshot.BASELINE_GEOMETRY):
        geom = snapshot.BASELINE_GEOMETRY[table]
        files = file_options(table, snapshot, parallelism)
        rgs = rg_options(table, snapshot)
        out[table] = {
            "baseline_files": geom["files"],
            "baseline_rg_per_file": geom.get("rg_per_file"),
            "compressed_gib": round(geom["compressed_bytes"] / 2 ** 30, 3),
            "file_options": [
                {"label": lab,
                 "target_file_bytes": b,
                 "n_files": geom["files"] if b is None
                            else max(1, -(-geom["compressed_bytes"] // b))}
                for lab, b in files],
            "rg_options": [
                {"label": lab, "rg_bytes": b,
                 "rg_mib": None if b is None else round(b / 2 ** 20, 1)}
                for lab, b in rgs],
        }
    return out


def main():
    import argparse
    import json
    import dataset_snapshot

    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset-snapshot", required=True)
    ap.add_argument("--parallelism", type=int, default=policy.PARALLELISM)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    snap = dataset_snapshot.load(args.dataset_snapshot)
    doc = describe(snap, args.parallelism)
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(doc, fh, indent=2)
    print(f"# adaptive physical options (parallelism {args.parallelism})")
    for table, rec in doc.items():
        files = ", ".join(
            f"{o['label']}({o['n_files']})" for o in rec["file_options"])
        rgs = ", ".join(
            f"{o['label']}({o['rg_mib']}MiB)" if o["rg_mib"] else o["label"]
            for o in rec["rg_options"])
        print(f"  {table:12s} {rec['compressed_gib']:8.2f}GiB "
              f"files=[{files}] rg=[{rgs}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
