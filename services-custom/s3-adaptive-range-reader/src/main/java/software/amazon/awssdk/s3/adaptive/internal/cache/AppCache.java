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

package software.amazon.awssdk.s3.adaptive.internal.cache;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.internal.budget.AppBudgetLease;
import software.amazon.awssdk.s3.adaptive.internal.budget.CacheEvictor;
import software.amazon.awssdk.s3.adaptive.internal.budget.GlobalBudget;

/**
 * The per-app (per query engine), multi-object block cache introduced in S3. Unlike the per-reader single-object
 * {@link PageCache}, it holds blocks for every object/version an app reads, keyed by {@code objectId} (which encodes
 * bucket/key + version token) and block start offset. Its byte footprint is charged to the shared
 * {@link GlobalBudget} through an {@link AppBudgetLease}, and it registers itself as the app's {@link CacheEvictor} so
 * the budget can reclaim its LRU blocks when another app needs its reserve back.
 *
 * <p>Isolation is structural: one instance per app, so an app only ever evicts (and is charged for) its own blocks.
 * Thread-safe across the app's concurrent readers by sharing the global budget's monitor for all mutations, which also
 * makes budget-driven eviction call-backs re-entrant. A read is a hit only when a single cached block for the same
 * object fully covers it (no cross-block stitching), matching {@link PageCache} / the simulator.
 */
@SdkInternalApi
public final class AppCache implements CacheEvictor {

    private final AppBudgetLease lease;
    private final GlobalBudget budget;
    // accessOrder=true -> eldest entry is the LRU victim.
    private final LinkedHashMap<BlockKey, CachedBlock> blocks = new LinkedHashMap<>(16, 0.75f, true);
    private long cachedBytes;

    public AppCache(AppBudgetLease lease) {
        this.lease = lease;
        this.budget = lease.globalBudget();
    }

    /**
     * Return a cached block of {@code objectId} fully covering {@code [start, end)} (touching LRU), or {@code null}.
     */
    public CachedBlock findCovering(String objectId, long start, long end) {
        if (end <= start) {
            return null;
        }
        synchronized (budget) {
            BlockKey exactKey = new BlockKey(objectId, start);
            CachedBlock exact = blocks.get(exactKey);
            if (exact != null && exact.end() >= end) {
                return exact;
            }
            BlockKey bestKey = null;
            CachedBlock best = null;
            for (Map.Entry<BlockKey, CachedBlock> entry : blocks.entrySet()) {
                BlockKey key = entry.getKey();
                CachedBlock block = entry.getValue();
                if (key.objectId.equals(objectId) && block.start() <= start && block.end() >= end) {
                    bestKey = key;
                    best = block;
                }
            }
            if (bestKey != null) {
                blocks.get(bestKey);
            }
            return best;
        }
    }

    /**
     * Insert a block for {@code objectId}, charging the shared budget (which may reclaim borrowed space from other
     * apps, then this app's own LRU). A block larger than the whole capacity is not cached.
     *
     * @return {@code true} if the block is now cached.
     */
    public boolean put(String objectId, long start, byte[] data) {
        if (data.length <= 0) {
            return false;
        }
        synchronized (budget) {
            BlockKey key = new BlockKey(objectId, start);
            CachedBlock previous = blocks.remove(key);
            if (previous != null) {
                cachedBytes -= previous.length();
                lease.release(previous.length());
            }
            if (data.length > budget.capacity() || !lease.acquire(data.length)) {
                return false;
            }
            blocks.put(key, new CachedBlock(start, data));
            cachedBytes += data.length;
            return true;
        }
    }

    @Override
    public long evictLru(long bytes) {
        synchronized (budget) {
            long freed = 0L;
            Iterator<Map.Entry<BlockKey, CachedBlock>> it = blocks.entrySet().iterator();
            while (freed < bytes && it.hasNext()) {
                Map.Entry<BlockKey, CachedBlock> eldest = it.next();
                freed += eldest.getValue().length();
                cachedBytes -= eldest.getValue().length();
                it.remove();
            }
            return freed;
        }
    }

    public long cachedBytes() {
        synchronized (budget) {
            return cachedBytes;
        }
    }

    public int blockCount() {
        synchronized (budget) {
            return blocks.size();
        }
    }

    private static final class BlockKey {
        private final String objectId;
        private final long start;

        private BlockKey(String objectId, long start) {
            this.objectId = objectId;
            this.start = start;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BlockKey)) {
                return false;
            }
            BlockKey other = (BlockKey) o;
            return start == other.start && objectId.equals(other.objectId);
        }

        @Override
        public int hashCode() {
            return 31 * objectId.hashCode() + Long.hashCode(start);
        }
    }
}
