#!/usr/bin/env python3
"""Does PROJECT2's cost proxy (GET*RTT + bytes/BW) rank configs the same way real
wall-clock does? Compares the assumed RTT=50ms/BW=100MiB/s against the RTT/BW
measured by E1, on the @256MiB configurations."""
from __future__ import annotations

import csv
import glob
import os
import sys

RESULTS = sys.argv[1] if len(sys.argv) > 1 else "."

# label -> pretty name, restricted to the 256MiB working point
WANT = {
    ("e3_four_b256_d0", "passthrough"): "passthrough",
    ("e3_four_b256_d0", "template_auto"): "template_auto",
    ("e3_four_b256_d0", "s2"): "decision_tree(S2)",
    ("e3_force_s3a_prefetch_b256", "forced_s3a_prefetch"): "static s3a_prefetch",
    ("e3_force_s3a_random_b256", "forced_s3a_random"): "static s3a_random",
    ("e3_force_template_locality_b256", "forced_template_locality"): "static template_locality",
    ("e3_force_template_multimodal_b256", "forced_template_multimodal"): "static template_multimodal",
    ("e3_oracle_cost_b256", "oracle_perworkload"): "oracle_perworkload",
}

MEASURED_RTT_S = 0.024987
MEASURED_BW_MIBPS = 102.278


def spearman(a, b):
    def rank(xs):
        order = sorted(range(len(xs)), key=lambda i: xs[i])
        r = [0.0] * len(xs)
        for pos, i in enumerate(order):
            r[i] = pos + 1
        return r
    ra, rb = rank(a), rank(b)
    n = len(a)
    d2 = sum((ra[i] - rb[i]) ** 2 for i in range(n))
    return 1 - 6 * d2 / (n * (n * n - 1))


def main():
    recs = []
    for f in glob.glob(os.path.join(RESULTS, "results-v2.*.csv")):
        if f.endswith("all.csv"):
            continue
        with open(f) as fh:
            for r in csv.DictReader(fh):
                if r["scope"] != "all":
                    continue
                key = (r["label"], r["mode"])
                if key in WANT:
                    recs.append((WANT[key], r))

    print(f"{'config':<28}{'GETs':>7}{'remMiB':>10}"
          f"{'proxy50':>10}{'proxy25':>10}{'wall_s':>9}")
    rows = []
    for name, r in recs:
        gets = int(r["remoteGets"])
        mib = float(r["remoteBytes"]) / 1048576
        wall = float(r["nsPerOp"]) * int(r["reads"]) / 1e9
        proxy50 = gets * 0.050 + mib / 100.0
        proxy25 = gets * MEASURED_RTT_S + mib / MEASURED_BW_MIBPS
        rows.append((name, gets, mib, proxy50, proxy25, wall))
    rows.sort(key=lambda x: x[5])
    for name, gets, mib, p50, p25, wall in rows:
        print(f"{name:<28}{gets:>7}{mib:>10.1f}{p50:>10.1f}{p25:>10.1f}{wall:>9.1f}")

    wall = [r[5] for r in rows]
    print(f"\nSpearman(proxy RTT=50ms/BW=100MiB/s , wall) = {spearman([r[3] for r in rows], wall):+.3f}")
    print(f"Spearman(proxy RTT=25ms/BW=102MiB/s , wall) = {spearman([r[4] for r in rows], wall):+.3f}")

    # absolute calibration
    print("\nabsolute error of each proxy vs measured wall-clock:")
    for name, _g, _m, p50, p25, w in rows:
        print(f"  {name:<28} proxy50={p50:7.1f} ({(p50-w)/w*100:+6.1f}%)   "
              f"proxy25={p25:7.1f} ({(p25-w)/w*100:+6.1f}%)")


if __name__ == "__main__":
    main()
