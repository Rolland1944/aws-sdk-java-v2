#!/usr/bin/env python3
"""Phase 3 §8: evidence that a run was not confounded by disk or page cache.

Isolation is measured, not assumed. For one command it records, per physical
disk behind each named directory, the read/write bytes and I/O counts the run
caused, plus the kernel page cache before and after, plus how much of each
watched file is resident in the page cache.

  python3 tools/track2/phase3_iso.py wrap --out run/iso.json \\
      --dir scratch=$HOME/track2-scratch --dir cache=/mnt/ssd-cache \\
      --watch /mnt/ssd-cache/cache.bin --drop-caches -- python3 ... run_benchmark.py ...

Checks written to the JSON ("checks"):
  separate_devices   scratch and cache directories sit on different disks
  cache_io_direct    cache disk traffic did not show up as page cache growth
                     (only meaningful once a cache file exists; see --watch)
Exit status is the wrapped command's. Any failed check is also printed.
"""
import argparse
import json
import os
import subprocess
import sys
import time

SECTOR = 512


def disk_of(path):
    os.makedirs(path, exist_ok=True)
    src = subprocess.check_output(["findmnt", "-n", "-o", "SOURCE", "--target", path],
                                  text=True).strip()
    parent = subprocess.run(["lsblk", "-no", "pkname", src], capture_output=True,
                            text=True).stdout.split()
    return parent[0] if parent else os.path.basename(src)


def diskstats(names):
    out = {}
    with open("/proc/diskstats") as fh:
        for line in fh:
            f = line.split()
            if f[2] in names:
                out[f[2]] = {"reads": int(f[3]), "read_bytes": int(f[5]) * SECTOR,
                             "writes": int(f[7]), "write_bytes": int(f[9]) * SECTOR}
    return out


def meminfo():
    keys = ("MemAvailable", "Cached", "Dirty")
    out = {}
    with open("/proc/meminfo") as fh:
        for line in fh:
            k, v = line.split(":")
            if k in keys:
                out[k] = int(v.split()[0]) * 1024
    return out


def resident(path):
    if not os.path.exists(path):
        return None
    rows = subprocess.check_output(
        ["fincore", "-b", "-n", "-o", "RES,SIZE", path], text=True).split()
    return {"resident_bytes": int(rows[0]), "size_bytes": int(rows[1])}


def drop_caches():
    subprocess.check_call(["sync"])
    subprocess.check_call(["sudo", "-n", "sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"])


def delta(a, b):
    return {d: {k: b[d][k] - a[d][k] for k in b[d]} for d in b}


def wrap(args):
    dirs = dict(x.split("=", 1) for x in args.dir)
    disks = {role: disk_of(path) for role, path in dirs.items()}
    names = set(disks.values())
    if args.drop_caches:
        drop_caches()
    before = {"disk": diskstats(names), "mem": meminfo(), "time": time.time()}
    rc = subprocess.call(args.cmd)
    after = {"disk": diskstats(names), "mem": meminfo(), "time": time.time(),
             "watch": {p: resident(p) for p in args.watch}}
    per_disk = delta(before["disk"], after["disk"])
    by_role = {role: {"disk": d, **per_disk[d]} for role, d in disks.items()}
    checks = {}
    if "scratch" in disks and "cache" in disks:
        checks["separate_devices"] = disks["scratch"] != disks["cache"]
    cache_write = by_role.get("cache", {}).get("write_bytes", 0)
    cache_read = by_role.get("cache", {}).get("read_bytes", 0)
    cached_growth = after["mem"]["Cached"] - before["mem"]["Cached"]
    if args.watch and cache_write + cache_read > (1 << 30):
        res = [w["resident_bytes"] for w in after["watch"].values() if w]
        checks["cache_io_direct"] = bool(res) and max(res) < 0.05 * (cache_write + cache_read) \
            and cached_growth < 0.25 * (cache_write + cache_read)
    report = {"cmd": args.cmd, "exit": rc, "seconds": after["time"] - before["time"],
              "dirs": dirs, "by_role": by_role, "mem_before": before["mem"],
              "mem_after": after["mem"], "watch": after["watch"], "checks": checks,
              "dropped_caches_before": args.drop_caches}
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as fh:
        json.dump(report, fh, indent=2)
    for role, v in by_role.items():
        print(f"# iso {role:8s} {v['disk']:10s} read={v['read_bytes'] / 2**30:.2f} GiB "
              f"write={v['write_bytes'] / 2**30:.2f} GiB", file=sys.stderr)
    for name, ok in checks.items():
        print(f"# iso check {name}: {'PASS' if ok else 'FAIL'}", file=sys.stderr)
    return rc


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="op", required=True)
    w = sub.add_parser("wrap")
    w.add_argument("--out", required=True)
    w.add_argument("--dir", action="append", default=[], help="role=path (scratch, cache, ...)")
    w.add_argument("--watch", action="append", default=[], help="file whose page-cache residency to report")
    w.add_argument("--drop-caches", action="store_true")
    w.add_argument("cmd", nargs=argparse.REMAINDER)
    args = ap.parse_args()
    if args.cmd and args.cmd[0] == "--":
        args.cmd = args.cmd[1:]
    if not args.cmd:
        ap.error("no command given")
    return wrap(args)


if __name__ == "__main__":
    sys.exit(main())
