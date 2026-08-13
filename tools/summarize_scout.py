#!/usr/bin/env python3
"""Summarize the scout-round real-S3 results into the tables PROJECT2 asks for."""
from __future__ import annotations

import csv
import glob
import os
import sys

RESULTS = sys.argv[1] if len(sys.argv) > 1 else "."


def load():
    rows = []
    for f in sorted(glob.glob(os.path.join(RESULTS, "results-v2.*.csv"))):
        if f.endswith("all.csv"):
            continue
        with open(f) as fh:
            for r in csv.DictReader(fh):
                r["_file"] = os.path.basename(f)
                rows.append(r)
    return rows


def wall_s(r):
    return float(r["nsPerOp"]) * int(r["reads"]) / 1e9


def fmt(rows, title, keyfn=None):
    print(f"\n=== {title} ===")
    print(f"{'label':<32}{'mode':<20}{'GETs':>7}{'remMiB':>10}{'amp':>8}"
          f"{'wall_s':>9}{'p50_ms':>9}{'p95_ms':>8}{'p99_ms':>8}{'clamp':>7}")
    for r in sorted(rows, key=keyfn) if keyfn else rows:
        print(f"{r['label']:<32}{r['mode']:<20}{int(r['remoteGets']):>7}"
              f"{float(r['remoteBytes'])/1048576:>10.1f}{float(r['readAmp']):>8.3f}"
              f"{wall_s(r):>9.1f}{float(r['p50Ns'])/1e6:>9.3f}"
              f"{float(r['p95Ns'])/1e6:>8.1f}{float(r['p99Ns'])/1e6:>8.1f}"
              f"{int(r['demandClampReads']):>7}")


def main():
    rows = [r for r in load() if r["scope"] == "all"]
    by_label = {}
    for r in rows:
        by_label.setdefault(r["label"], []).append(r)

    fmt([r for r in rows if r["label"].startswith("e2_")], "E2 baseline reproducibility")
    fmt([r for r in rows if r["label"].startswith("e3_four")], "E3 four-column")
    fmt([r for r in rows if r["label"].startswith("e3_force") or "oracle" in r["label"]],
        "E3 oracle_static / oracle_perworkload @256MiB")
    fmt([r for r in rows if r["label"].startswith("e4_")], "E4 g* block sweep",
        keyfn=lambda r: int(r["label"].split("_")[2].replace("KiB", "")))

    # E2 spread
    e2 = [r for r in rows if r["label"].startswith("e2_")]
    if len(e2) == 2:
        a, b = wall_s(e2[0]), wall_s(e2[1])
        print(f"\nE2 spread: {a:.1f}s vs {b:.1f}s -> {abs(a-b)/min(a,b)*100:.2f}%")

    # E3 ceiling per budget
    print("\n=== E3 ceiling vs budget (wall-clock) ===")
    print(f"{'budget':<10}{'passthru':>10}{'tmpl_auto':>11}{'S2(tree)':>10}"
          f"{'S2 vs pt':>10}{'tree vs rule':>14}")
    for b in ("128", "256", "1024"):
        lab = f"e3_four_b{b}_d0"
        got = {r["mode"]: r for r in by_label.get(lab, [])}
        if not got:
            continue
        pt = wall_s(got["passthrough"])
        ta = wall_s(got["template_auto"])
        cand = [m for m in got if m.lower().startswith("s2")]
        s2 = wall_s(got[cand[0]]) if cand else float("nan")
        print(f"{b+'MiB':<10}{pt:>10.1f}{ta:>11.1f}{s2:>10.1f}"
              f"{(s2-pt)/pt*100:>9.1f}%{(s2-ta)/ta*100:>13.1f}%")

    # oracle comparison
    print("\n=== oracle_static vs oracle_perworkload @256MiB (wall-clock) ===")
    forced = {r["label"]: wall_s(r) for r in rows if r["label"].startswith("e3_force")}
    for k, v in sorted(forced.items(), key=lambda kv: kv[1]):
        print(f"  {k:<40}{v:>9.1f}s")
    orc = [r for r in rows if "oracle" in r["label"]]
    if orc:
        o = wall_s(orc[0])
        best_static = min(forced.values()) if forced else None
        print(f"  {'oracle_perworkload(cost table)':<40}{o:>9.1f}s")
        if best_static:
            print(f"  => static->per-workload gap: {(best_static-o)/best_static*100:+.2f}%")

    # depth
    print("\n=== depth 0 vs 1 @256MiB ===")
    for lab in ("e3_four_b256_d0", "e3_four_b256_d1"):
        for r in by_label.get(lab, []):
            if r["mode"].lower().startswith("s3"):
                print(f"  {lab:<22}{r['mode']:<14}wall={wall_s(r):>7.1f}s "
                      f"GETs={r['remoteGets']:>6} prefetchUseful="
                      f"{float(r['prefetchUsefulBytes'])/1048576:.1f}MiB "
                      f"wasted={float(r['prefetchWastedBytes'])/1048576:.1f}MiB")


if __name__ == "__main__":
    main()
