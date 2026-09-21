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

import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Process-level counters for D1/D2/D4. GET/bytes still come from the
 * interceptor; these explain cache hits, merged GETs, waste and fallbacks.
 */
@SdkInternalApi
public final class Track1S3aStats {

    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final AtomicLong cacheUsefulBytes = new AtomicLong();
    private final AtomicLong remoteGets = new AtomicLong();
    private final AtomicLong mergedGets = new AtomicLong();
    private final AtomicLong mergedMembers = new AtomicLong();
    private final AtomicLong wastedBytes = new AtomicLong();
    private final AtomicLong queueWaitNanos = new AtomicLong();
    private final AtomicLong queueSubmissions = new AtomicLong();
    private final AtomicLong queueBatches = new AtomicLong();
    private final AtomicLong queueBatchTickets = new AtomicLong();
    private final AtomicLong queueSingletonBatches = new AtomicLong();
    private final AtomicLong sameObjectMultiTicketGroups = new AtomicLong();
    private final AtomicLong mergeableGroups = new AtomicLong();
    private final AtomicLong mergeBudgetFallbacks = new AtomicLong();
    private final AtomicLong maxBatchSize = new AtomicLong();
    private final AtomicLong maxSameObjectGroupSize = new AtomicLong();
    private final AtomicLong fallbacks = new AtomicLong();
    private final AtomicLong peakHeapBytes = new AtomicLong();
    private final AtomicLong admitRejected = new AtomicLong();
    private final AtomicLong teedGets = new AtomicLong();
    private final AtomicLong teedBytes = new AtomicLong();
    private final AtomicLong gcTimeMs = new AtomicLong();
    private final AtomicLong gcCount = new AtomicLong();

    public void cacheHit(long bytes) {
        cacheHits.incrementAndGet();
        cacheUsefulBytes.addAndGet(bytes);
        sampleHeap();
    }

    public void cacheMiss() {
        cacheMisses.incrementAndGet();
    }

    public void remoteGet() {
        remoteGets.incrementAndGet();
        sampleHeap();
    }

    public void merged(int members, long waste) {
        mergedGets.incrementAndGet();
        mergedMembers.addAndGet(members);
        wastedBytes.addAndGet(waste);
    }

    public void queueWait(long nanos) {
        if (nanos > 0) {
            queueWaitNanos.addAndGet(nanos);
        }
    }

    public void queueSubmitted() {
        queueSubmissions.incrementAndGet();
    }

    public void queueBatch(int tickets) {
        queueBatches.incrementAndGet();
        queueBatchTickets.addAndGet(tickets);
        if (tickets == 1) {
            queueSingletonBatches.incrementAndGet();
        }
        updateMax(maxBatchSize, tickets);
    }

    public void sameObjectGroup(int tickets) {
        if (tickets > 1) {
            sameObjectMultiTicketGroups.incrementAndGet();
            updateMax(maxSameObjectGroupSize, tickets);
        }
    }

    public void mergeableGroup() {
        mergeableGroups.incrementAndGet();
    }

    public void mergeBudgetFallback() {
        mergeBudgetFallbacks.incrementAndGet();
    }

    public void fallback() {
        fallbacks.incrementAndGet();
    }

    public void rejectAdmit() {
        admitRejected.incrementAndGet();
    }

    public void teedGet() {
        teedGet(0L);
    }

    public void teedGet(long bytes) {
        teedGets.incrementAndGet();
        if (bytes > 0) {
            teedBytes.addAndGet(bytes);
        }
    }

    public long cacheHits() {
        return cacheHits.get();
    }

    public long cacheMisses() {
        return cacheMisses.get();
    }

    public long cacheUsefulBytes() {
        return cacheUsefulBytes.get();
    }

    public long remoteGets() {
        return remoteGets.get();
    }

    public long mergedGets() {
        return mergedGets.get();
    }

    public long mergedMembers() {
        return mergedMembers.get();
    }

    public long wastedBytes() {
        return wastedBytes.get();
    }

    public long queueWaitNanos() {
        return queueWaitNanos.get();
    }

    public long queueSubmissions() {
        return queueSubmissions.get();
    }

    public long queueBatches() {
        return queueBatches.get();
    }

    public long queueBatchTickets() {
        return queueBatchTickets.get();
    }

    public long queueSingletonBatches() {
        return queueSingletonBatches.get();
    }

    public long sameObjectMultiTicketGroups() {
        return sameObjectMultiTicketGroups.get();
    }

    public long mergeableGroups() {
        return mergeableGroups.get();
    }

    public long mergeBudgetFallbacks() {
        return mergeBudgetFallbacks.get();
    }

    public long maxBatchSize() {
        return maxBatchSize.get();
    }

    public long maxSameObjectGroupSize() {
        return maxSameObjectGroupSize.get();
    }

    public long fallbacks() {
        return fallbacks.get();
    }

    public long admitRejected() {
        return admitRejected.get();
    }

    public long teedGets() {
        return teedGets.get();
    }

    public long teedBytes() {
        return teedBytes.get();
    }

    public long gcTimeMs() {
        return gcTimeMs.get();
    }

    public long gcCount() {
        return gcCount.get();
    }

    public long peakHeapBytes() {
        return peakHeapBytes.get();
    }

    public void sampleHeap() {
        try {
            long used = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
            long cur;
            do {
                cur = peakHeapBytes.get();
                if (used <= cur) {
                    return;
                }
            } while (!peakHeapBytes.compareAndSet(cur, used));
        } catch (Throwable ignored) {
            // Heap sampling must never fail a GET.
        }
    }

    /**
     * Process-lifetime GC totals. One Spark run is one JVM, so the snapshot
     * at {@code spark.stop()} is the run cost.
     */
    public void sampleGc() {
        try {
            long time = 0L;
            long count = 0L;
            for (java.lang.management.GarbageCollectorMXBean bean
                    : ManagementFactory.getGarbageCollectorMXBeans()) {
                long t = bean.getCollectionTime();
                long n = bean.getCollectionCount();
                if (t > 0) {
                    time += t;
                }
                if (n > 0) {
                    count += n;
                }
            }
            gcTimeMs.set(time);
            gcCount.set(count);
        } catch (Throwable ignored) {
            // GC sampling must never fail a GET.
        }
    }

    public void reset() {
        cacheHits.set(0);
        cacheMisses.set(0);
        cacheUsefulBytes.set(0);
        remoteGets.set(0);
        mergedGets.set(0);
        mergedMembers.set(0);
        wastedBytes.set(0);
        queueWaitNanos.set(0);
        queueSubmissions.set(0);
        queueBatches.set(0);
        queueBatchTickets.set(0);
        queueSingletonBatches.set(0);
        sameObjectMultiTicketGroups.set(0);
        mergeableGroups.set(0);
        mergeBudgetFallbacks.set(0);
        maxBatchSize.set(0);
        maxSameObjectGroupSize.set(0);
        fallbacks.set(0);
        admitRejected.set(0);
        teedGets.set(0);
        teedBytes.set(0);
        gcTimeMs.set(0);
        gcCount.set(0);
        peakHeapBytes.set(0);
    }

    private static void updateMax(AtomicLong target, long value) {
        long current;
        do {
            current = target.get();
            if (value <= current) {
                return;
            }
        } while (!target.compareAndSet(current, value));
    }
}
