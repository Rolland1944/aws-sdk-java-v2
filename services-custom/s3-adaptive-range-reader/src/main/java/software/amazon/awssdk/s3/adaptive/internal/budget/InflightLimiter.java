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

import java.util.concurrent.atomic.AtomicLong;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * A per-app ceiling on bytes of speculative prefetch in flight at once. Bounds the transient memory an app's
 * look-ahead can hold before those bytes land in the cache, independently of the reclaimable cache budget. Prefetch
 * that would exceed the cap is skipped (speculation is optional). Tracks the peak reservation.
 */
@SdkInternalApi
public final class InflightLimiter {

    private final long capBytes;
    private final AtomicLong current = new AtomicLong();
    private final AtomicLong peak = new AtomicLong();

    public InflightLimiter(long capBytes) {
        this.capBytes = Math.max(0L, capBytes);
    }

    /**
     * Reserve {@code bytes} if doing so stays within the cap.
     *
     * @return {@code true} if reserved (caller must later {@link #release(long)}).
     */
    public boolean tryReserve(long bytes) {
        if (bytes <= 0) {
            return true;
        }
        while (true) {
            long cur = current.get();
            long next = cur + bytes;
            if (next > capBytes) {
                return false;
            }
            if (current.compareAndSet(cur, next)) {
                peak.accumulateAndGet(next, Math::max);
                return true;
            }
        }
    }

    public void release(long bytes) {
        if (bytes > 0) {
            current.addAndGet(-bytes);
        }
    }

    public long current() {
        return current.get();
    }

    public long peak() {
        return peak.get();
    }

    public long capBytes() {
        return capBytes;
    }
}
