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

package software.amazon.awssdk.s3.adaptive.internal.metrics;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.PolicyName;
import software.amazon.awssdk.s3.adaptive.ReaderMetrics;

/**
 * Mutable per-reader accounting behind {@link ReaderMetrics}. The reader records read-level facts (policy chosen,
 * logical bytes, per-read latency, policy switches); executors record physical facts (remote GETs / bytes, cache
 * hits). Thread-safe: the S3 reader records demand facts on the caller thread while speculative prefetch completions
 * record on async IO threads, so all mutators and {@link #snapshot()} are synchronized.
 */
@SdkInternalApi
public final class MetricsRecorder {

    private final EnumMap<PolicyName, Long> policyReads = new EnumMap<>(PolicyName.class);
    private final List<Long> readLatenciesNanos = new ArrayList<>();

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
    private PolicyName lastPolicy;

    /**
     * Record one remote ranged GET of {@code bytes} bytes (demand IO).
     */
    public synchronized void recordRemoteFetch(long bytes) {
        remoteGets++;
        remoteBytes += bytes;
    }

    /**
     * Record one speculative prefetch GET. Prefetch bytes also count as remote bytes (they consume real bandwidth and
     * so are reflected in read amplification), and are additionally attributed to the prefetch counters.
     */
    public synchronized void recordPrefetchFetch(long bytes) {
        remoteGets++;
        remoteBytes += bytes;
        prefetchGets++;
        prefetchBytes += bytes;
    }

    /**
     * Record that {@code bytes} of a demand read were served from data that had been brought in speculatively.
     */
    public synchronized void recordPrefetchUseful(long bytes) {
        prefetchUsefulBytes += bytes;
    }

    /**
     * Record speculative bytes cancelled (e.g. on a {@code seek}) before they were ever consumed.
     */
    public synchronized void recordPrefetchCancelled(long bytes) {
        prefetchCancelledBytes += bytes;
    }

    /**
     * Record a read fully satisfied from cache.
     */
    public synchronized void recordCacheHitRead(long bytes) {
        cacheHitReads++;
        cacheHitBytes += bytes;
    }

    /**
     * Record that a planned demand range was reduced to the exact request
     * because it exceeded the configured single-fetch ceiling.
     */
    public synchronized void recordDemandClamp() {
        demandClampReads++;
    }

    /**
     * Record a logical read: the policy applied, bytes returned, latency, and whether it was a safety fallback.
     */
    public synchronized void recordRead(PolicyName policy, long bytes, long latencyNanos, boolean fallback) {
        logicalReads++;
        logicalBytes += bytes;
        readLatenciesNanos.add(latencyNanos);
        if (fallback) {
            fallbackReads++;
        }
        if (policy != null) {
            policyReads.merge(policy, 1L, Long::sum);
            if (lastPolicy != null && lastPolicy != policy) {
                policySwitches++;
            }
            lastPolicy = policy;
        }
    }

    public synchronized ReaderMetrics snapshot() {
        long[] latencies = new long[readLatenciesNanos.size()];
        for (int i = 0; i < latencies.length; i++) {
            latencies[i] = readLatenciesNanos.get(i);
        }
        Map<PolicyName, Long> reads = new EnumMap<>(PolicyName.class);
        reads.putAll(policyReads);
        return ReaderMetrics.builder()
                            .logicalReads(logicalReads)
                            .logicalBytes(logicalBytes)
                            .remoteGets(remoteGets)
                            .remoteBytes(remoteBytes)
                            .cacheHitReads(cacheHitReads)
                            .cacheHitBytes(cacheHitBytes)
                            .policySwitches(policySwitches)
                            .fallbackReads(fallbackReads)
                            .demandClampReads(demandClampReads)
                            .prefetchGets(prefetchGets)
                            .prefetchBytes(prefetchBytes)
                            .prefetchUsefulBytes(prefetchUsefulBytes)
                            .prefetchCancelledBytes(prefetchCancelledBytes)
                            .policyReadCounts(reads)
                            .latenciesNanos(latencies)
                            .build();
    }
}
