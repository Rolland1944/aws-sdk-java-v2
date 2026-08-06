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

package software.amazon.awssdk.s3.adaptive.internal.budget;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Process-level cap on the number of concurrently in-flight ranged GETs, shared across all apps. Demand fetches
 * {@link #acquire() block} for a permit (a demand read must eventually run); speculative prefetches use
 * {@link #tryAcquire()} and are simply skipped when the pool is saturated. Tracks the peak concurrency observed.
 */
@SdkInternalApi
public final class ConcurrencyLimiter {

    private final int maxConcurrent;
    private final Semaphore permits;
    private final AtomicInteger inUse = new AtomicInteger();
    private final AtomicInteger peak = new AtomicInteger();

    public ConcurrencyLimiter(int maxConcurrent) {
        this.maxConcurrent = Math.max(1, maxConcurrent);
        this.permits = new Semaphore(this.maxConcurrent);
    }

    public int maxConcurrent() {
        return maxConcurrent;
    }

    /**
     * Acquire a permit, blocking until one is available. For demand IO.
     */
    public void acquire() {
        permits.acquireUninterruptibly();
        track();
    }

    /**
     * Try to acquire a permit without blocking. For speculative prefetch.
     *
     * @return {@code true} if a permit was taken (caller must {@link #release()} it).
     */
    public boolean tryAcquire() {
        if (permits.tryAcquire()) {
            track();
            return true;
        }
        return false;
    }

    public void release() {
        inUse.decrementAndGet();
        permits.release();
    }

    public int peak() {
        return peak.get();
    }

    private void track() {
        peak.accumulateAndGet(inUse.incrementAndGet(), Math::max);
    }
}
