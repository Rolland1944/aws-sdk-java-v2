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

/**
 * A byte-budgeted, LRU block cache for a single object version. Mirrors the role of {@code BlockCache} in
 * {@code prefetch_simulator.py}: a read is a hit only when a single cached block fully covers it (no cross-block
 * stitching), and inserting past the byte budget evicts least-recently-used blocks.
 *
 * <p>Blocks are keyed by their {@code start} offset. Because executors fetch ranges that always begin at a policy
 * boundary and are stored whole, a covering block for {@code [start, end)} is the most-recently-used block with
 * {@code block.start <= start} and {@code block.end >= end}. Version isolation is achieved by construction: one
 * cache instance per pinned version; a new version gets a fresh cache.
 *
 * <p>Not thread-safe (one reader/stream per instance).
 */
@SdkInternalApi
public final class PageCache {

    private final long budgetBytes;
    // accessOrder=true -> iteration/eldest reflects LRU; we evict from the front.
    private final LinkedHashMap<Long, CachedBlock> blocks = new LinkedHashMap<>(16, 0.75f, true);
    private long cachedBytes;

    public PageCache(long budgetBytes) {
        this.budgetBytes = budgetBytes;
    }

    /**
     * Return a cached block fully covering {@code [start, end)} (touching LRU), or {@code null} on a miss.
     */
    public CachedBlock findCovering(long start, long end) {
        if (end <= start) {
            return null;
        }
        // Exact-start hit is the common case (page/block-aligned fetches).
        CachedBlock exact = blocks.get(start);
        if (exact != null && exact.end() >= end) {
            return exact;
        }
        // Otherwise scan for any block that covers the request. The cache holds few, large blocks, so a linear scan
        // is cheap; correctness (never returning partial coverage) matters more than asymptotics here.
        CachedBlock best = null;
        for (CachedBlock block : blocks.values()) {
            if (block.start() <= start && block.end() >= end) {
                best = block;
            }
        }
        if (best != null) {
            // Mark as most-recently-used.
            blocks.get(best.start());
        }
        return best;
    }

    /**
     * Insert a block, evicting LRU blocks until within budget. A block larger than the whole budget is not cached
     * (it would evict everything and still not fit), but callers still get the bytes they fetched.
     */
    public void put(CachedBlock block) {
        if (block.length() <= 0) {
            return;
        }
        CachedBlock previous = blocks.remove(block.start());
        if (previous != null) {
            cachedBytes -= previous.length();
        }
        if (block.length() > budgetBytes) {
            return;
        }
        blocks.put(block.start(), block);
        cachedBytes += block.length();
        evictToBudget();
    }

    private void evictToBudget() {
        Iterator<Map.Entry<Long, CachedBlock>> it = blocks.entrySet().iterator();
        while (cachedBytes > budgetBytes && it.hasNext()) {
            Map.Entry<Long, CachedBlock> eldest = it.next();
            cachedBytes -= eldest.getValue().length();
            it.remove();
        }
    }

    public long cachedBytes() {
        return cachedBytes;
    }

    public int blockCount() {
        return blocks.size();
    }
}
