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

package software.amazon.awssdk.s3.adaptive.s3a;

import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Online RTT / bandwidth used by D4: {@code g* = RTT × BW}. Until a few samples
 * land, {@code g*} stays at a conservative 128 KiB so a cold start cannot merge
 * a multi-megabyte hole.
 */
@SdkInternalApi
public final class LinkEstimator {

    static final long DEFAULT_G_STAR = 128L * 1024;
    private static final long SMALL_GET = 32L * 1024;
    private static final double ALPHA = 0.2;
    private static final long MIN_TRANSFER_NS = 200_000L;

    private final long minGStar;
    private final long maxGStar;
    private double rttNanos = Double.NaN;
    private double bwBytesPerSec = Double.NaN;
    private int samples;

    public LinkEstimator(long maxGStar) {
        this.minGStar = 64L * 1024;
        this.maxGStar = Math.max(this.minGStar, maxGStar);
    }

    public synchronized void record(long bytes, long latencyNanos) {
        if (bytes <= 0 || latencyNanos <= 0) {
            return;
        }
        samples++;
        if (bytes <= SMALL_GET || Double.isNaN(rttNanos)) {
            rttNanos = ewma(rttNanos, latencyNanos);
        }
        long transfer = latencyNanos;
        if (!Double.isNaN(rttNanos)) {
            transfer = Math.max(MIN_TRANSFER_NS, latencyNanos - (long) rttNanos);
        }
        double instBw = bytes * 1_000_000_000.0 / transfer;
        bwBytesPerSec = ewma(bwBytesPerSec, instBw);
    }

    public synchronized long gStarBytes() {
        if (samples < 3 || Double.isNaN(rttNanos) || Double.isNaN(bwBytesPerSec)) {
            return DEFAULT_G_STAR;
        }
        long raw = (long) ((rttNanos / 1_000_000_000.0) * bwBytesPerSec);
        if (raw < minGStar) {
            return minGStar;
        }
        if (raw > maxGStar) {
            return maxGStar;
        }
        return raw;
    }

    public synchronized int samples() {
        return samples;
    }

    public synchronized double rttNanos() {
        return Double.isNaN(rttNanos) ? 0.0 : rttNanos;
    }

    public synchronized double bwBytesPerSec() {
        return Double.isNaN(bwBytesPerSec) ? 0.0 : bwBytesPerSec;
    }

    private static double ewma(double current, double sample) {
        if (Double.isNaN(current)) {
            return sample;
        }
        return ALPHA * sample + (1.0 - ALPHA) * current;
    }
}
