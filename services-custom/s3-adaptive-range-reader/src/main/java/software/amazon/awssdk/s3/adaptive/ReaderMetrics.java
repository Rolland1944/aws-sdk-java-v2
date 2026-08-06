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

package software.amazon.awssdk.s3.adaptive;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import software.amazon.awssdk.annotations.SdkPublicApi;

/**
 * An immutable snapshot of an {@link AdaptiveRangeReader}'s IO accounting: logical vs remote bytes, GET count, read
 * amplification, cache hit rate, per-policy read counts, policy switches, and per-read latency percentiles.
 */
@SdkPublicApi
public final class ReaderMetrics {

    private final long logicalReads;
    private final long logicalBytes;
    private final long remoteGets;
    private final long remoteBytes;
    private final long cacheHitReads;
    private final long cacheHitBytes;
    private final long policySwitches;
    private final long fallbackReads;
    private final long demandClampReads;
    private final long prefetchGets;
    private final long prefetchBytes;
    private final long prefetchUsefulBytes;
    private final long prefetchCancelledBytes;
    private final Map<PolicyName, Long> policyReadCounts;
    private final long[] sortedLatenciesNanos;

    private ReaderMetrics(Builder b) {
        this.logicalReads = b.logicalReads;
        this.logicalBytes = b.logicalBytes;
        this.remoteGets = b.remoteGets;
        this.remoteBytes = b.remoteBytes;
        this.cacheHitReads = b.cacheHitReads;
        this.cacheHitBytes = b.cacheHitBytes;
        this.policySwitches = b.policySwitches;
        this.fallbackReads = b.fallbackReads;
        this.demandClampReads = b.demandClampReads;
        this.prefetchGets = b.prefetchGets;
        this.prefetchBytes = b.prefetchBytes;
        this.prefetchUsefulBytes = b.prefetchUsefulBytes;
        this.prefetchCancelledBytes = b.prefetchCancelledBytes;
        Map<PolicyName, Long> counts = b.policyReadCounts == null ? new EnumMap<>(PolicyName.class) : b.policyReadCounts;
        this.policyReadCounts = Collections.unmodifiableMap(new EnumMap<>(counts));
        this.sortedLatenciesNanos = b.latenciesNanos == null ? new long[0] : b.latenciesNanos.clone();
        Arrays.sort(this.sortedLatenciesNanos);
    }

    public static Builder builder() {
        return new Builder();
    }

    public long logicalReads() {
        return logicalReads;
    }

    public long logicalBytes() {
        return logicalBytes;
    }

    public long remoteGets() {
        return remoteGets;
    }

    public long remoteBytes() {
        return remoteBytes;
    }

    public long cacheHitReads() {
        return cacheHitReads;
    }

    public long cacheHitBytes() {
        return cacheHitBytes;
    }

    public long policySwitches() {
        return policySwitches;
    }

    public long fallbackReads() {
        return fallbackReads;
    }

    /**
     * Demand reads whose planner range exceeded the configured single-fetch
     * ceiling and therefore fell back to the exact requested range.
     */
    public long demandClampReads() {
        return demandClampReads;
    }

    /**
     * Number of speculative prefetch GETs issued.
     */
    public long prefetchGets() {
        return prefetchGets;
    }

    /**
     * Bytes fetched speculatively (a subset of {@link #remoteBytes()}).
     */
    public long prefetchBytes() {
        return prefetchBytes;
    }

    /**
     * Prefetched bytes later consumed by a demand read (the payoff of prefetching).
     */
    public long prefetchUsefulBytes() {
        return prefetchUsefulBytes;
    }

    /**
     * Speculative bytes cancelled (e.g. on a {@code seek}) before being consumed.
     */
    public long prefetchCancelledBytes() {
        return prefetchCancelledBytes;
    }

    /**
     * Prefetched bytes that completed but were never consumed by a demand read (waste). Derived as
     * {@code max(0, prefetchBytes - prefetchUsefulBytes)}.
     */
    public long prefetchWastedBytes() {
        return Math.max(0L, prefetchBytes - prefetchUsefulBytes);
    }

    /**
     * Per-policy read counts (only policies that were actually used appear).
     */
    public Map<PolicyName, Long> policyReadCounts() {
        return policyReadCounts;
    }

    /**
     * Read amplification: remote bytes fetched divided by logical bytes served. 1.0 is ideal (no over-read);
     * {@code NaN} if no logical bytes were read.
     */
    public double readAmplification() {
        return logicalBytes == 0 ? Double.NaN : (double) remoteBytes / logicalBytes;
    }

    /**
     * Fraction of logical reads fully served from cache.
     */
    public double cacheHitRate() {
        return logicalReads == 0 ? Double.NaN : (double) cacheHitReads / logicalReads;
    }

    public long latencyP50Nanos() {
        return percentileNanos(50);
    }

    public long latencyP95Nanos() {
        return percentileNanos(95);
    }

    public long latencyP99Nanos() {
        return percentileNanos(99);
    }

    /**
     * Nearest-rank percentile of per-read latency in nanoseconds; 0 if no reads were recorded.
     */
    public long percentileNanos(int percentile) {
        if (sortedLatenciesNanos.length == 0) {
            return 0L;
        }
        int rank = (int) Math.ceil(percentile / 100.0 * sortedLatenciesNanos.length);
        int idx = Math.min(sortedLatenciesNanos.length - 1, Math.max(0, rank - 1));
        return sortedLatenciesNanos[idx];
    }

    @Override
    public String toString() {
        return "ReaderMetrics{logicalReads=" + logicalReads
               + ", logicalBytes=" + logicalBytes
               + ", remoteGets=" + remoteGets
               + ", remoteBytes=" + remoteBytes
               + ", readAmplification=" + readAmplification()
               + ", cacheHitRate=" + cacheHitRate()
               + ", policySwitches=" + policySwitches
               + ", fallbackReads=" + fallbackReads
               + ", demandClampReads=" + demandClampReads
               + ", prefetchGets=" + prefetchGets
               + ", prefetchBytes=" + prefetchBytes
               + ", prefetchUsefulBytes=" + prefetchUsefulBytes
               + ", prefetchWastedBytes=" + prefetchWastedBytes()
               + ", prefetchCancelledBytes=" + prefetchCancelledBytes
               + ", policyReadCounts=" + policyReadCounts
               + ", p50Ns=" + latencyP50Nanos()
               + ", p95Ns=" + latencyP95Nanos()
               + ", p99Ns=" + latencyP99Nanos()
               + '}';
    }

    /**
     * Mutable builder for {@link ReaderMetrics}. All fields default to zero / empty.
     */
    @SdkPublicApi
    public static final class Builder {

        private long logicalReads;
        private long logicalBytes;
        private long remoteGets;
        private long remoteBytes;
        private long cacheHitReads;
        private long cacheHitBytes;
        private long policySwitches;
        private long fallbackReads;
        private long demandClampReads;
        private long prefetchGets;
        private long prefetchBytes;
        private long prefetchUsefulBytes;
        private long prefetchCancelledBytes;
        private Map<PolicyName, Long> policyReadCounts;
        private long[] latenciesNanos;

        private Builder() {
        }

        public Builder logicalReads(long logicalReads) {
            this.logicalReads = logicalReads;
            return this;
        }

        public Builder logicalBytes(long logicalBytes) {
            this.logicalBytes = logicalBytes;
            return this;
        }

        public Builder remoteGets(long remoteGets) {
            this.remoteGets = remoteGets;
            return this;
        }

        public Builder remoteBytes(long remoteBytes) {
            this.remoteBytes = remoteBytes;
            return this;
        }

        public Builder cacheHitReads(long cacheHitReads) {
            this.cacheHitReads = cacheHitReads;
            return this;
        }

        public Builder cacheHitBytes(long cacheHitBytes) {
            this.cacheHitBytes = cacheHitBytes;
            return this;
        }

        public Builder policySwitches(long policySwitches) {
            this.policySwitches = policySwitches;
            return this;
        }

        public Builder fallbackReads(long fallbackReads) {
            this.fallbackReads = fallbackReads;
            return this;
        }

        public Builder demandClampReads(long demandClampReads) {
            this.demandClampReads = demandClampReads;
            return this;
        }

        public Builder prefetchGets(long prefetchGets) {
            this.prefetchGets = prefetchGets;
            return this;
        }

        public Builder prefetchBytes(long prefetchBytes) {
            this.prefetchBytes = prefetchBytes;
            return this;
        }

        public Builder prefetchUsefulBytes(long prefetchUsefulBytes) {
            this.prefetchUsefulBytes = prefetchUsefulBytes;
            return this;
        }

        public Builder prefetchCancelledBytes(long prefetchCancelledBytes) {
            this.prefetchCancelledBytes = prefetchCancelledBytes;
            return this;
        }

        public Builder policyReadCounts(Map<PolicyName, Long> policyReadCounts) {
            this.policyReadCounts = policyReadCounts;
            return this;
        }

        public Builder latenciesNanos(long[] latenciesNanos) {
            this.latenciesNanos = latenciesNanos;
            return this;
        }

        public ReaderMetrics build() {
            return new ReaderMetrics(this);
        }
    }
}
