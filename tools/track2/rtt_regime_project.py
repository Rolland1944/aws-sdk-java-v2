#!/usr/bin/env python3
"""Project Track1's wall-clock gain to higher per-GET latency regimes.

No new measurement: every input is a number already in TRACK1.md. The point of
the script is that the projection is reproducible and its assumptions are in one
place, not that it is precise -- the output is a band, and the band is wide.

The chain is:
  1. Solve `ttfb_sum = n_get * ttfb + gib / bw` on the two extreme cells of the
     D1 budget sweep (TRACK1.md 5.2). Two cells, two unknowns, so per-GET cost
     and effective bandwidth fall out of the sweep itself rather than being
     assumed.
  2. `io_union = request_time / K_busy`, where K_busy is measured, not modelled
     (TRACK1.md 6.1 gives union and ttfb_sum per cell, and their ratio is K).
  3. Wall clock is bounded by `max(C, union) <= wall <= C + union`, where C is
     the compute floor `task CPU / cores`. The lower end assumes I/O and compute
     overlap perfectly, the upper end assumes they serialise. The measured
     same-region point sits inside the band, which is the only check available.

The band is reported for two K assumptions because K is the one quantity that
could plausibly change with latency, and the two available measurements
disagree about it (4.42 same-region on ClickBench, 6.77 at 228 ms on TPC-H).
Both are shown rather than averaged: if the conclusion needs the average it is
not a conclusion.

Usage:
  python3 tools/track2/rtt_regime_project.py
"""

from __future__ import annotations

# ---- TRACK1.md 5.2, D1 budget sweep: (cell, n_get, remote_gib, ttfb_sum_s, io_union_s)
SWEEP = (
    ("000", 23421, 31.62, 669.4, 151.5),
    ("100", 8370, 30.69, 261.7, 85.4),
    ("100-4m1g", 4855, 25.73, 167.2, 66.1),
    ("100-8m2g", 4334, 22.57, 154.6, 62.1),
)

# task CPU core-seconds from the sweep's event logs (TRACK1.md 6.1), over
# local[16]. This is the floor no read-path change can go below.
CORES = 16
CPU_CORE_S = {"000": 2891.6, "100": 2972.9, "100-8m2g": 3000.4}

# TCP connect to s3.<region>.amazonaws.com from the m5dn.4xlarge in us-east-2,
# i.e. one network RTT. Same-region is 2.2 ms, so the rest of the fitted 26 ms
# per-GET cost is S3's own first-byte floor and does not scale with distance.
REGION_RTT_S = (
    ("us-east-2 (measured)", 0.0000),
    ("us-east-1", 0.0104),
    ("us-west-2", 0.0498),
    ("eu-west-1", 0.0762),
    ("eu-central-1", 0.0973),
    ("ap-southeast-1", 0.2156),
)


def fit_per_get_cost():
    """Per-GET cost and seconds-per-GiB, from the two extreme sweep cells."""
    (_, n1, g1, t1, _), (_, n2, g2, t2, _) = SWEEP[0], SWEEP[-1]
    det = n1 * g2 - n2 * g1
    return (t1 * g2 - t2 * g1) / det, (n1 * t2 - n2 * t1) / det


def main():
    ttfb0, s_per_gib = fit_per_get_cost()
    base, cand = SWEEP[0], SWEEP[-1]
    c_base = CPU_CORE_S[base[0]] / CORES
    c_cand = CPU_CORE_S[cand[0]] / CORES
    k_base, k_cand = base[3] / base[4], cand[3] / cand[4]

    print("fitted from the D1 sweep: per-GET %.1f ms, %.0f MiB/s effective"
          % (ttfb0 * 1000, 1024 / s_per_gib))
    print("network RTT to same-region S3 is 2.2 ms, so ~%.0f ms of that is "
          "S3's first-byte floor" % (ttfb0 * 1000 - 2.2))
    print()
    print("request time = latency + transfer, per cell:")
    for name, n, gib, ttfb_sum, union in SWEEP:
        lat, xfer = n * ttfb0, gib * s_per_gib
        print("  %-10s GET %6d  latency %6.1fs (%4.1f%%)  transfer %5.1fs  K_busy %.2f"
              % (name, n, lat, 100 * lat / (lat + xfer), xfer, ttfb_sum / union))
    print()

    for label, kb, kc in (("K as measured same-region", k_base, k_cand),
                          ("K = 6.77 (measured at 228 ms)", 6.77, 6.77)):
        print("== %s ==" % label)
        print("  %-22s %8s %11s %11s %16s"
              % ("region", "per-GET", "000 union", "8m2g union", "Track1 gain"))
        for region, extra in REGION_RTT_S:
            ttfb = ttfb0 + extra
            u_base = (base[1] * ttfb + base[2] * s_per_gib) / kb
            u_cand = (cand[1] * ttfb + cand[2] * s_per_gib) / kc
            lo = 1 - (c_cand + u_cand) / max(c_base, u_base)
            hi = 1 - max(c_cand, u_cand) / (c_base + u_base)
            lo, hi = sorted((lo * 100, hi * 100))
            print("  %-22s %6.0fms %10.0fs %10.0fs %7.0f%%..%.0f%%"
                  % (region, ttfb * 1000, u_base, u_cand, lo, hi))
        print()

    print("compute floor %.0fs (000) / %.0fs (8m2g); union below that is "
          "entirely hideable." % (c_base, c_cand))


if __name__ == "__main__":
    main()
