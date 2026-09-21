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

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Rolling window of exact-range reads used to estimate the reusable working
 * set. Distinct bytes are unique {@code (object, start, end)} lengths still
 * in the window; reusable bytes are those ranges seen at least twice. No
 * query id is stored.
 *
 * <p>The window rolls by a byte horizon first so a long scan cannot collapse
 * the estimator onto the last few thousand events. A max-event cap is only a
 * safety bound on map size.
 */
@SdkInternalApi
public final class WorkingSetWindow {

    public static final long DEFAULT_HORIZON_BYTES = 4L * 1024L * 1024L * 1024L;
    public static final int DEFAULT_MAX_EVENTS = 131072;
    public static final long DEFAULT_OVERSIZE_MARK = 256L * 1024L;

    private final long horizonBytes;
    private final int maxEvents;
    private final long oversizeMark;
    private final Deque<RangeKey> order = new ArrayDeque<RangeKey>();
    private final Map<RangeKey, Integer> counts = new HashMap<RangeKey, Integer>();
    private final Map<RangeKey, Long> lastSeq = new HashMap<RangeKey, Long>();
    private final long[] reusedSizes;
    private int reusedSizeCount;
    private int reusedSizeHead;
    private long seq;
    private long distinctBytes;
    private long reusableBytes;
    private long eventBytes;
    private long oversizeEventBytes;

    public WorkingSetWindow() {
        this(DEFAULT_HORIZON_BYTES, DEFAULT_MAX_EVENTS, DEFAULT_OVERSIZE_MARK);
    }

    /**
     * Count-capped window used by tests. Byte horizon is unlimited.
     */
    public WorkingSetWindow(int maxEvents) {
        this(Long.MAX_VALUE, Math.max(16, maxEvents), DEFAULT_OVERSIZE_MARK);
    }

    public WorkingSetWindow(long horizonBytes, int maxEvents) {
        this(horizonBytes, maxEvents, DEFAULT_OVERSIZE_MARK);
    }

    public WorkingSetWindow(long horizonBytes, int maxEvents, long oversizeMark) {
        this.horizonBytes = horizonBytes <= 0L ? Long.MAX_VALUE : horizonBytes;
        this.maxEvents = Math.max(1, maxEvents);
        this.oversizeMark = Math.max(0L, oversizeMark);
        this.reusedSizes = new long[256];
    }

    public synchronized void observe(String objectId, long start, long length) {
        if (length <= 0 || objectId == null) {
            return;
        }
        RangeKey key = new RangeKey(objectId, start, start + length);
        seq++;
        Long prev = lastSeq.put(key, seq);
        if (prev != null) {
            recordReusedSize(length);
        }
        Integer before = counts.get(key);
        if (before == null) {
            counts.put(key, 1);
            distinctBytes += length;
        } else {
            int next = before + 1;
            counts.put(key, next);
            if (before == 1) {
                reusableBytes += length;
            }
        }
        order.addLast(key);
        eventBytes += length;
        if (oversizeMark > 0L && length > oversizeMark) {
            oversizeEventBytes += length;
        }
        evictWhileOver();
    }

    public synchronized boolean seenBefore(String objectId, long start, long length) {
        if (length <= 0 || objectId == null) {
            return false;
        }
        return counts.containsKey(new RangeKey(objectId, start, start + length));
    }

    public synchronized long distinctBytes() {
        return distinctBytes;
    }

    public synchronized long reusableBytes() {
        return reusableBytes;
    }

    public synchronized long eventBytes() {
        return eventBytes;
    }

    public synchronized double pollution() {
        if (eventBytes <= 0L || oversizeMark <= 0L) {
            return 0.0;
        }
        return (double) oversizeEventBytes / (double) eventBytes;
    }

    public long horizonBytes() {
        return horizonBytes;
    }

    public int maxEvents() {
        return maxEvents;
    }

    public long oversizeMark() {
        return oversizeMark;
    }

    public synchronized int size() {
        return order.size();
    }

    public synchronized long reusedSizePercentile(double q) {
        int n = Math.min(reusedSizeCount, reusedSizes.length);
        if (n == 0) {
            return 0L;
        }
        long[] copy = Arrays.copyOf(reusedSizes, n);
        Arrays.sort(copy);
        int idx = (int) Math.floor(q * (n - 1));
        if (idx < 0) {
            idx = 0;
        }
        if (idx >= n) {
            idx = n - 1;
        }
        return copy[idx];
    }

    public synchronized void reset() {
        order.clear();
        counts.clear();
        lastSeq.clear();
        reusedSizeCount = 0;
        reusedSizeHead = 0;
        seq = 0;
        distinctBytes = 0;
        reusableBytes = 0;
        eventBytes = 0;
        oversizeEventBytes = 0;
    }

    private void evictWhileOver() {
        while (!order.isEmpty()
               && (order.size() > maxEvents || eventBytes > horizonBytes)) {
            evictOldest();
        }
    }

    private void evictOldest() {
        RangeKey key = order.removeFirst();
        long length = key.length();
        eventBytes -= length;
        if (eventBytes < 0L) {
            eventBytes = 0L;
        }
        if (oversizeMark > 0L && length > oversizeMark) {
            oversizeEventBytes -= length;
            if (oversizeEventBytes < 0L) {
                oversizeEventBytes = 0L;
            }
        }
        Integer count = counts.get(key);
        if (count == null) {
            return;
        }
        if (count <= 1) {
            counts.remove(key);
            lastSeq.remove(key);
            distinctBytes -= length;
            if (distinctBytes < 0) {
                distinctBytes = 0;
            }
            return;
        }
        int next = count - 1;
        counts.put(key, next);
        if (next == 1) {
            reusableBytes -= length;
            if (reusableBytes < 0) {
                reusableBytes = 0;
            }
        }
    }

    private void recordReusedSize(long length) {
        reusedSizes[reusedSizeHead] = length;
        reusedSizeHead = (reusedSizeHead + 1) % reusedSizes.length;
        if (reusedSizeCount < reusedSizes.length) {
            reusedSizeCount++;
        }
    }

    private static final class RangeKey {
        private final String objectId;
        private final long start;
        private final long end;

        private RangeKey(String objectId, long start, long end) {
            this.objectId = objectId;
            this.start = start;
            this.end = end;
        }

        private long length() {
            return end - start;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof RangeKey)) {
                return false;
            }
            RangeKey other = (RangeKey) o;
            return start == other.start && end == other.end && objectId.equals(other.objectId);
        }

        @Override
        public int hashCode() {
            int h = objectId.hashCode();
            h = 31 * h + Long.hashCode(start);
            return 31 * h + Long.hashCode(end);
        }
    }
}
