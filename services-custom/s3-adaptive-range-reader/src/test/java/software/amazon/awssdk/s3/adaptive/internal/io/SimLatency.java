/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package software.amazon.awssdk.s3.adaptive.internal.io;

import java.util.concurrent.locks.LockSupport;

/**
 * Shared "remote read cost" model for the benchmark object-store doubles: per-GET latency is {@code rtt + bytes / BW}
 * (same shape as {@code prefetch_simulator.CostModel}). Used to inject real, size-aware delays so wall-clock / p50-p99
 * reflect network cost - only then does prefetch's latency-hiding (invisible on a zero-RTT store) become measurable.
 */
public final class SimLatency {

    private static final double MIB = 1024.0 * 1024.0;

    private SimLatency() {
    }

    /**
     * Nanoseconds a GET of {@code bytes} should take: {@code rttNanos} fixed + transfer time at {@code bwMiBps}
     * ({@code <= 0} means "no bandwidth limit", i.e. only the fixed RTT applies).
     */
    public static long fetchNanos(long rttNanos, double bwMiBps, long bytes) {
        long nanos = Math.max(0L, rttNanos);
        if (bwMiBps > 0 && bytes > 0) {
            nanos += (long) (bytes / (bwMiBps * MIB) * 1_000_000_000.0);
        }
        return nanos;
    }

    /**
     * Sleep precisely for {@code nanos} (park-until-deadline; robust to spurious wakeups). No-op for {@code <= 0}.
     */
    public static void sleepNanos(long nanos) {
        if (nanos <= 0) {
            return;
        }
        long deadline = System.nanoTime() + nanos;
        long remaining = nanos;
        while (remaining > 0) {
            LockSupport.parkNanos(remaining);
            remaining = deadline - System.nanoTime();
        }
    }
}
