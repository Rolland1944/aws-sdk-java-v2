#!/usr/bin/env python3
"""Phase 3 §8 B1.1: random-read latency of each candidate cache device.

fio, O_DIRECT (page cache cannot serve any read), libaio. One preallocated
file per device, written with direct I/O first so every block is backed.

  python3 tools/track2/phase3_device_bench.py \\
      --out docs/adaptive-range-reader/results/phase3/ssd_device_bench_aws_20261015 \\
      --target ebs-root=$HOME/ssd-bench --target nvme-a=/mnt/ssd-cache/bench \\
      --target nvme-b=/mnt/ssd-spare/bench

Output: fio_<target>_<bs>_qd<n>.json per job, summary.json, summary.md.
Latency is fio completion latency (submit to complete), in ms.
"""
import argparse
import json
import os
import platform
import subprocess
import sys

KIB = 1024
SIZES = {"64k": 64 * KIB, "256k": 256 * KIB, "1m": 1024 * KIB, "4m": 4096 * KIB, "8m": 8192 * KIB}


def fio(args, name, path, bs, qd, rw, runtime, out_json):
    cmd = ["fio", f"--name={name}", f"--filename={path}", f"--size={args.size_gib}g",
           "--direct=1", "--ioengine=libaio", f"--rw={rw}", f"--bs={bs}",
           f"--iodepth={qd}", "--numjobs=1", "--randrepeat=0", "--group_reporting",
           "--output-format=json", f"--output={out_json}"]
    if runtime:
        cmd += ["--time_based", f"--runtime={runtime}", f"--ramp_time={args.ramp}"]
    subprocess.check_call(cmd)


def pick(job, key):
    side = job["read"]
    c = side["clat_ns"]
    pct = c["percentile"]
    return {
        "iops": side["iops"], "mib_s": side["bw_bytes"] / 2**20,
        "p50_ms": pct["50.000000"] / 1e6, "p99_ms": pct["99.000000"] / 1e6,
        "p999_ms": pct["99.900000"] / 1e6, "mean_ms": c["mean"] / 1e6,
        "ios": side["total_ios"],
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", required=True)
    ap.add_argument("--target", action="append", required=True, help="label=file path")
    ap.add_argument("--size-gib", type=int, default=8)
    ap.add_argument("--runtime", type=int, default=15)
    ap.add_argument("--ramp", type=int, default=2)
    ap.add_argument("--sizes", default=",".join(SIZES))
    ap.add_argument("--qds", default="1,4,16")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    sizes = [s for s in args.sizes.split(",") if s]
    qds = [int(x) for x in args.qds.split(",")]
    targets = dict(t.split("=", 1) for t in args.target)

    for label, path in targets.items():
        os.makedirs(os.path.dirname(path), exist_ok=True)
        print(f"# layout {label} {path} {args.size_gib} GiB", flush=True)
        fio(args, f"layout-{label}", path, "1m", 8, "write", 0,
            os.path.join(args.out, f"layout_{label}.json"))

    rows = []
    for label, path in targets.items():
        for bs in sizes:
            for qd in qds:
                out = os.path.join(args.out, f"fio_{label}_{bs}_qd{qd}.json")
                print(f"# {label} bs={bs} qd={qd}", flush=True)
                fio(args, f"{label}-{bs}-{qd}", path, bs, qd, "randread", args.runtime, out)
                with open(out) as fh:
                    job = json.load(fh)["jobs"][0]
                rows.append({"target": label, "bs": bs, "bs_bytes": SIZES[bs], "qd": qd,
                             **pick(job, "read")})

    meta = {
        "host": platform.node(),
        "instance_type": os.environ.get("INSTANCE_TYPE", "m5dn.4xlarge"),
        "kernel": platform.release(),
        "fio": subprocess.check_output(["fio", "--version"], text=True).strip(),
        "targets": targets, "size_gib": args.size_gib, "runtime_s": args.runtime,
        "ramp_s": args.ramp, "direct": True, "ioengine": "libaio",
        "block_devices": subprocess.check_output(
            ["lsblk", "-o", "NAME,SIZE,MODEL,FSTYPE,MOUNTPOINT"], text=True),
    }
    with open(os.path.join(args.out, "summary.json"), "w") as fh:
        json.dump({"meta": meta, "rows": rows}, fh, indent=2)
    with open(os.path.join(args.out, "summary.md"), "w") as fh:
        fh.write("| target | bs | qd | p50 ms | p99 ms | p99.9 ms | MiB/s | IOPS |\n")
        fh.write("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n")
        for r in rows:
            fh.write(f"| {r['target']} | {r['bs']} | {r['qd']} | {r['p50_ms']:.3f} | "
                     f"{r['p99_ms']:.3f} | {r['p999_ms']:.3f} | {r['mib_s']:.0f} | "
                     f"{r['iops']:.0f} |\n")
    print(open(os.path.join(args.out, "summary.md")).read())
    return 0


if __name__ == "__main__":
    sys.exit(main())
