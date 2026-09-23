/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 */

package software.amazon.awssdk.s3.adaptive.internal.cache;

import java.util.Locale;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Fixed-size service-time estimator for D1 cache value. Samples are S3 setup
 * plus blocking read time, not application wall time between reads.
 */
@SdkInternalApi
public final class RemoteCostEstimator {
    private static final int SIZE_BUCKETS = 16;
    private static final int MIN_SAMPLES = 8;
    private static final double K = 1.0;
    private final Bucket pooled = new Bucket();
    private final Bucket[] buckets = new Bucket[SIZE_BUCKETS];

    public RemoteCostEstimator() {
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new Bucket();
        }
    }

    public synchronized void record(long bytes, long nanos) {
        if (bytes <= 0 || nanos <= 0) {
            return;
        }
        long clipped = Math.min(nanos, 30_000_000_000L);
        pooled.record(clipped);
        buckets[bucket(bytes)].record(clipped);
    }

    public synchronized boolean mature(long bytes) {
        return buckets[bucket(bytes)].ready();
    }

    public synchronized double safeCost(long bytes) {
        Bucket b = buckets[bucket(bytes)];
        double pooledCost = pooled.ready() ? pooled.lcb() : 1.0;
        if (!b.ready()) {
            return pooledCost;
        }
        double confidence = Math.min(1.0, (double) b.count / (double) (MIN_SAMPLES * 8));
        return confidence * b.lcb() + (1.0 - confidence) * pooledCost;
    }

    public synchronized long samples() {
        return pooled.count;
    }

    public synchronized double meanNanos() {
        return pooled.mean;
    }

    public synchronized String snapshotFragment() {
        return "\"d1_cost_samples\":" + pooled.count
            + ",\"d1_cost_mean_ns\":" + (long) pooled.mean
            + ",\"d1_cost_lcb_ns\":" + (long) (pooled.ready() ? pooled.lcb() : 0L)
            + ",\"d1_cost_buckets\":\"" + bucketSummary() + "\"";
    }

    private String bucketSummary() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < buckets.length; i++) {
            Bucket b = buckets[i];
            if (b.count > 0) {
                if (sb.length() > 0) {
                    sb.append(';');
                }
                sb.append(i).append(':').append(b.count).append(':')
                  .append(String.format(Locale.ROOT, "%.0f", b.mean));
            }
        }
        return sb.toString();
    }

    private static int bucket(long bytes) {
        long v = Math.max(1L, bytes);
        int b = 0;
        while (v > 1L && b < SIZE_BUCKETS - 1) {
            v >>= 1;
            b++;
        }
        return b;
    }

    private static final class Bucket {
        private long count;
        private double mean;
        private double m2;

        private void record(long value) {
            count++;
            double delta = value - mean;
            mean += delta / count;
            double delta2 = value - mean;
            m2 += delta * delta2;
        }

        private boolean ready() {
            return count >= MIN_SAMPLES;
        }

        private double lcb() {
            if (count <= 1) {
                return Math.max(1.0, mean);
            }
            double variance = Math.max(0.0, m2 / (count - 1));
            double stderr = Math.sqrt(variance / count);
            return Math.max(1.0, mean - K * stderr);
        }
    }
}
