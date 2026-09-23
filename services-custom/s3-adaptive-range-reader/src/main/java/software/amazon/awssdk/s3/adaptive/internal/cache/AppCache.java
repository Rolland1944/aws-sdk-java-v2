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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
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

    public enum ReplacementPolicy {
        LRU,
        WTINYLFU
    }

    private final AppBudgetLease lease;
    private final GlobalBudget budget;
    // accessOrder=true -> eldest entry is the LRU victim.
    private final LinkedHashMap<BlockKey, CachedBlock> blocks = new LinkedHashMap<>(16, 0.75f, true);
    // Start-offset index per object, so a covering lookup does not scan every block of every object.
    private final Map<String, NavigableMap<Long, CachedBlock>> byObject = new HashMap<>();
    private long maxBlockLength;
    private long cachedBytes;
    private long peakCachedBytes;
    private long evictedBytes;
    private long evictedBlocks;
    private long admittedBlocks;
    private long rejectedBlocks;
    private long rejectGhostHits;
    private long rejectRequestGhostHits;
    private long evictionGhostHits;
    private int lastVictimCount;
    private long lastVictimBytes;
    private double lastVictimBenefit;
    private long windowBytes;
    private long probationBytes;
    private long protectedBytes;
    private long sketchBytes = 16L * 1024L * 1024L;
    private ReplacementPolicy policy = ReplacementPolicy.LRU;
    private final Map<BlockKey, EntryMeta> metas = new HashMap<>();
    private final Map<CachedBlock, Integer> pins = new IdentityHashMap<CachedBlock, Integer>();
    private final Map<BlockKey, GhostMeta> rejectGhosts = new LinkedHashMap<BlockKey, GhostMeta>(16, 0.75f, true);
    private final Map<BlockKey, GhostMeta> evictionGhosts = new LinkedHashMap<BlockKey, GhostMeta>(16, 0.75f, true);
    private final Map<BlockKey, Integer> frequency = new HashMap<>();
    private final RequestSizeHistogram requestSizes = new RequestSizeHistogram();
    private final LinkedHashSet<BlockKey> window = new LinkedHashSet<>();
    private final LinkedHashSet<BlockKey> probation = new LinkedHashSet<>();
    private final LinkedHashSet<BlockKey> protectedQ = new LinkedHashSet<>();
    private final RemoteCostEstimator remoteCosts = new RemoteCostEstimator();
    // Soft target under the hard GlobalBudget. Long.MAX_VALUE means "use hard
    // capacity only". Bypass is target 0: stop admitting and evict toward empty.
    private long softCapacity = Long.MAX_VALUE;

    public AppCache(AppBudgetLease lease) {
        this.lease = lease;
        this.budget = lease.globalBudget();
    }

    public void setReplacementPolicy(ReplacementPolicy policy) {
        synchronized (budget) {
            this.policy = policy == null ? ReplacementPolicy.LRU : policy;
        }
    }

    public void recordRemoteCost(long bytes, long nanos) {
        remoteCosts.record(bytes, nanos);
    }

    /**
     * Bounded first-touch filter for adaptive D1. A first observation is
     * intentionally served without allocating a cache payload; subsequent
     * observations may tee and compete for admission.
     */
    public boolean seenByDoorkeeper(String objectId, long start) {
        synchronized (budget) {
            BlockKey key = new BlockKey(objectId, start);
            incrementFrequency(key);
            return frequency.getOrDefault(key, 0) > 1;
        }
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
                touch(exactKey, exact.length());
                return exact;
            }
            NavigableMap<Long, CachedBlock> starts = byObject.get(objectId);
            if (starts == null) {
                return null;
            }
            // A covering block starts at or before start, and no earlier than start - maxBlockLength.
            for (CachedBlock block : starts.subMap(start - maxBlockLength, true, start, true)
                                          .descendingMap()
                                          .values()) {
                if (block.end() >= end) {
                    BlockKey key = new BlockKey(objectId, block.start());
                    blocks.get(key);
                    touch(key, block.length());
                    return block;
                }
            }
            return null;
        }
    }

    /**
     * Resolve and pin all blocks covering one range while holding the cache
     * lock once. Pinned blocks are skipped by eviction until the caller closes
     * the returned range.
     */
    public PinnedRange pinCoveringSpan(String objectId, long start, long end) {
        if (end <= start) {
            return null;
        }
        synchronized (budget) {
            List<CachedBlock> span = new ArrayList<CachedBlock>();
            NavigableMap<Long, CachedBlock> starts = byObject.get(objectId);
            long position = start;
            while (position < end) {
                CachedBlock block = findCoveringUnlocked(objectId, starts, position, Math.min(end, position + 1));
                if (block == null || block.end() <= position) {
                    for (CachedBlock pinned : span) {
                        Integer count = pins.get(pinned);
                        if (count == null || count <= 1) {
                            pins.remove(pinned);
                        } else {
                            pins.put(pinned, count - 1);
                        }
                    }
                    return null;
                }
                span.add(block);
                pins.put(block, pins.getOrDefault(block, 0) + 1);
                position = Math.min(end, block.end());
            }
            return new PinnedRange(this, span, start, end);
        }
    }

    private CachedBlock findCoveringUnlocked(String objectId, NavigableMap<Long, CachedBlock> starts,
                                             long start, long end) {
        BlockKey exactKey = new BlockKey(objectId, start);
        CachedBlock exact = blocks.get(exactKey);
        if (exact != null && exact.end() >= end) {
            touch(exactKey, exact.length());
            return exact;
        }
        if (starts == null) {
            return null;
        }
        for (CachedBlock block : starts.subMap(start - maxBlockLength, true, start, true)
                                      .descendingMap().values()) {
            if (block.end() >= end) {
                BlockKey key = new BlockKey(objectId, block.start());
                blocks.get(key);
                touch(key, block.length());
                return block;
            }
        }
        return null;
    }

    private void releasePins(List<CachedBlock> span) {
        synchronized (budget) {
            for (CachedBlock block : span) {
                Integer count = pins.get(block);
                if (count == null || count <= 1) {
                    pins.remove(block);
                } else {
                    pins.put(block, count - 1);
                }
            }
        }
    }

    /**
     * Insert a block for {@code objectId}, charging the shared budget (which may reclaim borrowed space from other
     * apps, then this app's own LRU). A block larger than the whole capacity is not cached.
     *
     * @return {@code true} if the block is now cached.
     */
    public boolean put(String objectId, long start, byte[] data) {
        return put(objectId, start, data, data == null ? 0L : data.length);
    }

    /**
     * Insert a block attributed to an original request of {@code originBytes}.
     * Pieces created by splitting one GET keep that request length so admission
     * telemetry does not collapse them onto the physical block size.
     */
    public boolean put(String objectId, long start, byte[] data, long originBytes) {
        if (data == null || data.length <= 0) {
            return false;
        }
        long origin = originBytes > 0L ? originBytes : data.length;
        synchronized (budget) {
            if (policy == ReplacementPolicy.WTINYLFU) {
                return putAdaptive(objectId, start, data, origin);
            }
            return putLru(objectId, start, data, origin);
        }
    }

    /**
     * Atomically admits every physical piece of an original GET or none of
     * them. This keeps a multi-block request from being partly cached while
     * later pieces lose a different victim comparison.
     */
    public boolean putRequest(String objectId, long start, byte[] data, long blockBytes) {
        return putRequest(objectId, start, data, blockBytes, false);
    }

    /**
     * Same as {@link #putRequest(String, long, byte[], long)}, but transfers
     * exclusive ownership of {@code data} to the cache on admission. Callers
     * must not mutate it after a successful call.
     */
    public boolean putRequestOwned(String objectId, long start, byte[] data, long blockBytes) {
        return putRequest(objectId, start, data, blockBytes, true);
    }

    private boolean putRequest(String objectId, long start, byte[] data, long blockBytes, boolean owned) {
        if (data == null || data.length <= 0 || blockBytes <= 0L) {
            return false;
        }
        synchronized (budget) {
            if (policy != ReplacementPolicy.WTINYLFU) {
                return putRequestLru(objectId, start, data, blockBytes, owned);
            }
            return putRequestAdaptive(objectId, start, data, blockBytes, owned);
        }
    }

    private boolean putRequestLru(String objectId, long start, byte[] data, long blockBytes, boolean owned) {
        long soft = effectiveSoftCapacity();
        if (data.length > soft || data.length > budget.capacity()) {
            rejectedBlocks += pieces(start, data, blockBytes).size();
            return false;
        }
        List<Piece> pieces = pieces(start, data, blockBytes);
        Set<BlockKey> replacements = new HashSet<BlockKey>();
        long replacementBytes = 0L;
        for (Piece piece : pieces) {
            BlockKey key = new BlockKey(objectId, piece.start);
            CachedBlock previous = blocks.get(key);
            if (previous != null && replacements.add(key)) {
                replacementBytes += previous.length();
            }
        }
        long free = soft - cachedBytes + replacementBytes;
        List<BlockKey> victims = planLruVictims(replacements, Math.max(0L, data.length - free));
        long victimBytes = 0L;
        for (BlockKey victim : victims) {
            victimBytes += blocks.get(victim).length();
        }
        if (victimBytes + Math.max(0L, free) < data.length) {
            rejectedBlocks += pieces.size();
            return false;
        }
        if (!owned) {
            data = Arrays.copyOf(data, data.length);
        }
        for (BlockKey victim : victims) {
            releaseDetached(victim, true);
        }
        for (BlockKey replacement : replacements) {
            releaseDetached(replacement, false);
        }
        if (!lease.acquire(data.length)) {
            rejectedBlocks += pieces.size();
            return false;
        }
        for (Piece piece : pieces) {
            insert(objectId, new BlockKey(objectId, piece.start), piece.start, data, piece.offset, piece.length,
                   data.length);
        }
        return true;
    }

    private boolean putRequestAdaptive(String objectId, long start, byte[] data, long blockBytes, boolean owned) {
        BlockKey requestKey = new BlockKey(objectId, start);
        if (rejectGhosts.remove(requestKey) != null) {
            rejectGhostHits++;
            rejectRequestGhostHits++;
        }
        if (evictionGhosts.remove(requestKey) != null) {
            evictionGhostHits++;
        }
        incrementFrequency(requestKey);
        List<Piece> pieces = pieces(start, data, blockBytes);
        long requestBytes = data.length;
        long soft = effectiveSoftCapacity();
        if (requestBytes > soft || requestBytes > budget.capacity()) {
            rejectedBlocks += pieces.size();
            rememberReject(requestKey, requestBytes);
            return false;
        }

        Set<BlockKey> replacements = new HashSet<BlockKey>();
        long replacementBytes = 0L;
        for (Piece piece : pieces) {
            BlockKey key = new BlockKey(objectId, piece.start);
            CachedBlock previous = blocks.get(key);
            if (previous != null && replacements.add(key)) {
                replacementBytes += previous.length();
            }
        }
        long free = soft - cachedBytes + replacementBytes;
        List<BlockKey> victims = new ArrayList<BlockKey>();
        if (free < requestBytes) {
            long needed = requestBytes - Math.max(0L, free);
            victims = planVictims(replacements, needed);
            long victimBytes = 0L;
            double victimBenefit = 0.0;
            for (BlockKey victim : victims) {
                CachedBlock block = blocks.get(victim);
                if (block != null) {
                    victimBytes += block.length();
                    victimBenefit += benefit(victim, block.length());
                }
            }
            lastVictimCount = victims.size();
            lastVictimBytes = victimBytes;
            lastVictimBenefit = victimBenefit;
            if (victimBytes < needed || benefit(requestKey, requestBytes) < victimBenefit) {
                rejectedBlocks += pieces.size();
                rememberReject(requestKey, requestBytes);
                return false;
            }
        } else {
            lastVictimCount = 0;
            lastVictimBytes = 0L;
            lastVictimBenefit = 0.0;
        }

        for (BlockKey victim : victims) {
            releaseDetached(victim, true);
        }
        for (BlockKey replacement : replacements) {
            releaseDetached(replacement, false);
        }
        if (!lease.acquire(requestBytes)) {
            rejectedBlocks += pieces.size();
            rememberReject(requestKey, requestBytes);
            return false;
        }
        if (!owned) {
            data = Arrays.copyOf(data, data.length);
        }
        for (Piece piece : pieces) {
            insert(objectId, new BlockKey(objectId, piece.start), piece.start, data, piece.offset, piece.length,
                   requestBytes);
        }
        return true;
    }

    private boolean putLru(String objectId, long start, byte[] data, long origin) {
        return putLru(objectId, start, data, 0, data.length, origin);
    }

    private boolean putLru(String objectId, long start, byte[] data, int offset, int length, long origin) {
        BlockKey key = new BlockKey(objectId, start);
        long freed = detach(key, false);
        if (freed > 0L) {
            lease.release(freed);
        }
        long soft = effectiveSoftCapacity();
        if (length > soft || length > budget.capacity()) {
            rejectedBlocks++;
            return false;
        }
        trimTo(soft - length);
        if (!lease.acquire(length)) {
            rejectedBlocks++;
            return false;
        }
        return insert(objectId, key, start, data, offset, length, origin);
    }

    private boolean putAdaptive(String objectId, long start, byte[] data, long origin) {
        BlockKey key = new BlockKey(objectId, start);
        noteGhostOnAccess(key);
        incrementFrequency(key);
        long soft = effectiveSoftCapacity();
        if (data.length > soft || data.length > budget.capacity()) {
            rejectedBlocks++;
            rememberReject(key, data.length);
            return false;
        }
        CachedBlock previous = blocks.get(key);
        long previousLen = previous == null ? 0L : previous.length();
        long free = soft - cachedBytes + previousLen;
        List<BlockKey> victims = new ArrayList<BlockKey>();
        if (free < data.length) {
            long needed = data.length - Math.max(0L, free);
            victims = planVictims(key, needed);
            long victimBytes = 0L;
            double victimBenefit = 0.0;
            for (BlockKey victim : victims) {
                CachedBlock block = blocks.get(victim);
                if (block != null) {
                    victimBytes += block.length();
                    victimBenefit += benefit(victim, block.length());
                }
            }
            lastVictimCount = victims.size();
            lastVictimBytes = victimBytes;
            lastVictimBenefit = victimBenefit;
            // Protected entries are not eligible. A candidate that cannot pay
            // for this exact set is rejected without changing the cache.
            if (victimBytes < needed || benefit(key, data.length) < victimBenefit) {
                rejectedBlocks++;
                rememberReject(key, data.length);
                return false;
            }
        } else {
            lastVictimCount = 0;
            lastVictimBytes = 0L;
            lastVictimBenefit = 0.0;
        }
        for (BlockKey victim : victims) {
            long freed = detach(victim, true);
            if (freed > 0L) {
                lease.release(freed);
            }
        }
        if (blocks.containsKey(key)) {
            long freed = detach(key, false);
            if (freed > 0L) {
                lease.release(freed);
            }
        }
        if (!lease.acquire(data.length)) {
            rejectedBlocks++;
            rememberReject(key, data.length);
            return false;
        }
        return insert(objectId, key, start, data, origin);
    }

    private boolean insert(String objectId, BlockKey key, long start, byte[] data, long origin) {
        return insert(objectId, key, start, data, 0, data.length, origin);
    }

    private boolean insert(String objectId, BlockKey key, long start, byte[] data, int offset, int length,
                           long origin) {
        CachedBlock block = new CachedBlock(start, data, offset, length);
        blocks.put(key, block);
        byObject.computeIfAbsent(objectId, id -> new TreeMap<>()).put(start, block);
        addMeta(key, length, origin);
        maxBlockLength = Math.max(maxBlockLength, block.length());
        cachedBytes += length;
        admittedBlocks++;
        if (cachedBytes > peakCachedBytes) {
            peakCachedBytes = cachedBytes;
        }
        return true;
    }

    private void releaseDetached(BlockKey key, boolean evictionGhost) {
        long freed = detach(key, evictionGhost);
        if (freed > 0L) {
            lease.release(freed);
        }
    }

    /**
     * Lower or raise the soft target. Raising never evicts. Lowering evicts
     * LRU blocks until {@link #cachedBytes()} fits, without rebuilding the map.
     */
    public void setSoftCapacity(long bytes) {
        synchronized (budget) {
            if (bytes == Long.MAX_VALUE) {
                this.softCapacity = Long.MAX_VALUE;
                return;
            }
            this.softCapacity = Math.max(0L, Math.min(bytes, budget.capacity()));
            trimTo(this.softCapacity);
        }
    }

    public long softCapacity() {
        synchronized (budget) {
            return effectiveSoftCapacity();
        }
    }

    @Override
    public long evictLru(long bytes) {
        synchronized (budget) {
            return evictLruUnlocked(bytes);
        }
    }

    private void trimTo(long maxCached) {
        long over = cachedBytes - Math.max(0L, maxCached);
        if (over > 0) {
            long freed = evictLruUnlocked(over);
            if (freed > 0) {
                lease.release(freed);
            }
        }
    }

    private long evictLruUnlocked(long bytes) {
        long freed = 0L;
        while (freed < bytes && !blocks.isEmpty()) {
            BlockKey victim = policy == ReplacementPolicy.WTINYLFU ? policyVictim() : lruVictim();
            if (victim == null) {
                break;
            }
            freed += evictBlock(victim);
        }
        return freed;
    }

    private BlockKey lruVictim() {
        for (Map.Entry<BlockKey, CachedBlock> entry : blocks.entrySet()) {
            if (!isPinned(entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    private BlockKey policyVictim() {
        BlockKey key = firstEvictable(probation);
        if (key != null) {
            return key;
        }
        key = firstEvictable(window);
        if (key != null) {
            return key;
        }
        key = firstEvictable(protectedQ);
        if (key != null) {
            return key;
        }
        return lruVictim();
    }

    private static BlockKey first(LinkedHashSet<BlockKey> set) {
        Iterator<BlockKey> it = set.iterator();
        return it.hasNext() ? it.next() : null;
    }

    private BlockKey firstEvictable(LinkedHashSet<BlockKey> set) {
        for (BlockKey key : set) {
            CachedBlock block = blocks.get(key);
            if (block != null && !isPinned(block)) {
                return key;
            }
        }
        return null;
    }

    private boolean isPinned(CachedBlock block) {
        return pins.containsKey(block);
    }

    private long detach(BlockKey key, boolean evictionGhost) {
        CachedBlock block = blocks.remove(key);
        if (block == null) {
            removeMeta(key);
            return 0L;
        }
        long len = block.length();
        cachedBytes -= len;
        if (evictionGhost) {
            evictedBytes += len;
            evictedBlocks++;
            rememberEviction(key, len);
        }
        unindex(key.objectId, block.start());
        removeMeta(key);
        return len;
    }

    private long evictBlock(BlockKey key) {
        return detach(key, true);
    }

    private void addMeta(BlockKey key, long bytes, long origin) {
        metas.put(key, new EntryMeta(bytes, origin, Segment.WINDOW));
        requestSizes.addResident(origin, bytes);
        if (policy != ReplacementPolicy.WTINYLFU) {
            return;
        }
        window.add(key);
        windowBytes += bytes;
        enforceSegments();
    }

    private void touch(BlockKey key, long bytes) {
        incrementFrequency(key);
        if (policy != ReplacementPolicy.WTINYLFU) {
            return;
        }
        EntryMeta meta = metas.get(key);
        if (meta == null) {
            metas.put(key, new EntryMeta(bytes, bytes, Segment.PROBATION));
            probation.add(key);
            probationBytes += bytes;
            return;
        }
        if (meta.segment == Segment.PROTECTED) {
            moveToTail(protectedQ, key);
        } else {
            removeFromSegment(key, meta);
            meta.segment = Segment.PROTECTED;
            protectedQ.add(key);
            protectedBytes += meta.bytes;
            enforceProtected();
        }
    }

    private void enforceSegments() {
        long maxWindow = windowTarget();
        while (windowBytes > maxWindow && !window.isEmpty()) {
            BlockKey key = first(window);
            EntryMeta meta = metas.get(key);
            if (meta == null) {
                window.remove(key);
            } else {
                removeFromSegment(key, meta);
                meta.segment = Segment.PROBATION;
                probation.add(key);
                probationBytes += meta.bytes;
            }
        }
        enforceProtected();
    }

    private void enforceProtected() {
        long main = Math.max(0L, effectiveSoftCapacity() - windowTarget());
        long maxProtected = (long) (main * 0.80);
        while (protectedBytes > maxProtected && !protectedQ.isEmpty()) {
            BlockKey key = first(protectedQ);
            EntryMeta meta = metas.get(key);
            if (meta == null) {
                protectedQ.remove(key);
            } else {
                removeFromSegment(key, meta);
                meta.segment = Segment.PROBATION;
                probation.add(key);
                probationBytes += meta.bytes;
            }
        }
    }

    private void removeMeta(BlockKey key) {
        EntryMeta meta = metas.remove(key);
        if (meta != null) {
            requestSizes.addResident(meta.originBytes, -meta.bytes);
            removeFromSegment(key, meta);
        } else {
            window.remove(key);
            probation.remove(key);
            protectedQ.remove(key);
        }
    }

    private void removeFromSegment(BlockKey key, EntryMeta meta) {
        switch (meta.segment) {
            case WINDOW:
                if (window.remove(key)) {
                    windowBytes -= meta.bytes;
                }
                break;
            case PROBATION:
                if (probation.remove(key)) {
                    probationBytes -= meta.bytes;
                }
                break;
            case PROTECTED:
                if (protectedQ.remove(key)) {
                    protectedBytes -= meta.bytes;
                }
                break;
            default:
                break;
        }
        if (windowBytes < 0L) {
            windowBytes = 0L;
        }
        if (probationBytes < 0L) {
            probationBytes = 0L;
        }
        if (protectedBytes < 0L) {
            protectedBytes = 0L;
        }
    }

    private static void moveToTail(LinkedHashSet<BlockKey> set, BlockKey key) {
        if (set.remove(key)) {
            set.add(key);
        }
    }

    private void incrementFrequency(BlockKey key) {
        int before = frequency.getOrDefault(key, 0);
        if (before < 15) {
            frequency.put(key, before + 1);
        }
        if (frequency.size() > 262144) {
            ageFrequency();
        }
    }

    private void ageFrequency() {
        Iterator<Map.Entry<BlockKey, Integer>> it = frequency.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<BlockKey, Integer> e = it.next();
            int next = e.getValue() / 2;
            if (next <= 0) {
                it.remove();
            } else {
                e.setValue(next);
            }
        }
    }

    private double benefit(BlockKey key, long bytes) {
        int freq = Math.max(1, frequency.getOrDefault(key, 1));
        if (remoteCosts.mature(bytes)) {
            return freq * remoteCosts.safeCost(bytes);
        }
        return freq * (double) bytes;
    }

    private List<BlockKey> planVictims(BlockKey candidate, long needed) {
        Set<BlockKey> excluded = new HashSet<BlockKey>();
        excluded.add(candidate);
        return planVictims(excluded, needed);
    }

    private List<BlockKey> planLruVictims(Set<BlockKey> excluded, long needed) {
        List<BlockKey> plan = new ArrayList<BlockKey>();
        long freed = 0L;
        for (Map.Entry<BlockKey, CachedBlock> entry : blocks.entrySet()) {
            if (freed >= needed) {
                break;
            }
            if (!excluded.contains(entry.getKey()) && !isPinned(entry.getValue())) {
                plan.add(entry.getKey());
                freed += entry.getValue().length();
            }
        }
        return plan;
    }

    private List<BlockKey> planVictims(Set<BlockKey> excluded, long needed) {
        List<BlockKey> plan = new ArrayList<BlockKey>();
        long got = takeVictims(probation, excluded, needed, 0L, plan);
        if (got < needed) {
            takeVictims(window, excluded, needed, got, plan);
        }
        return plan;
    }

    private long takeVictims(LinkedHashSet<BlockKey> queue, Set<BlockKey> excluded, long needed, long got,
                             List<BlockKey> plan) {
        for (BlockKey key : queue) {
            if (got >= needed) {
                break;
            }
            CachedBlock block = blocks.get(key);
            if (excluded.contains(key) || block == null || isPinned(block)) {
                continue;
            }
            plan.add(key);
            got += block.length();
        }
        return got;
    }

    private static List<Piece> pieces(long start, byte[] data, long blockBytes) {
        List<Piece> pieces = new ArrayList<Piece>();
        int offset = 0;
        while (offset < data.length) {
            long absolute = start + offset;
            long nextBoundary = ((absolute / blockBytes) + 1L) * blockBytes;
            int length = (int) Math.min(data.length - offset, nextBoundary - absolute);
            pieces.add(new Piece(absolute, offset, length));
            offset += length;
        }
        return pieces;
    }

    private long windowTarget() {
        long soft = effectiveSoftCapacity();
        if (soft <= 0L) {
            return 0L;
        }
        return Math.max(1L, Math.min(soft, Math.max(soft / 100L, Math.min(1024L * 1024L, soft / 10L))));
    }

    private void noteGhostOnAccess(BlockKey key) {
        if (evictionGhosts.remove(key) != null) {
            evictionGhostHits++;
        }
        if (rejectGhosts.remove(key) != null) {
            rejectGhostHits++;
        }
    }

    private void rememberReject(BlockKey key, long bytes) {
        evictionGhosts.remove(key);
        rejectGhosts.put(key, new GhostMeta(bytes, "reject"));
        trimGhosts(rejectGhosts);
    }

    private void rememberEviction(BlockKey key, long bytes) {
        rejectGhosts.remove(key);
        evictionGhosts.put(key, new GhostMeta(bytes, "evict"));
        trimGhosts(evictionGhosts);
    }

    private static void trimGhosts(Map<BlockKey, GhostMeta> ghosts) {
        while (ghosts.size() > 65536) {
            Iterator<BlockKey> it = ghosts.keySet().iterator();
            if (!it.hasNext()) {
                return;
            }
            it.next();
            it.remove();
        }
    }

    private long effectiveSoftCapacity() {
        return softCapacity == Long.MAX_VALUE ? budget.capacity()
                                              : Math.min(softCapacity, budget.capacity());
    }

    public long cachedBytes() {
        synchronized (budget) {
            return cachedBytes;
        }
    }

    public long peakCachedBytes() {
        synchronized (budget) {
            return peakCachedBytes;
        }
    }

    public long evictedBytes() {
        synchronized (budget) {
            return evictedBytes;
        }
    }

    public long evictedBlocks() {
        synchronized (budget) {
            return evictedBlocks;
        }
    }

    public long admittedBlocks() {
        synchronized (budget) {
            return admittedBlocks;
        }
    }

    public long rejectedBlocks() {
        synchronized (budget) {
            return rejectedBlocks;
        }
    }

    public long ghostHits() {
        synchronized (budget) {
            return evictionGhostHits;
        }
    }

    public long rejectGhostHits() {
        synchronized (budget) {
            return rejectGhostHits;
        }
    }

    public long rejectRequestGhostHits() {
        synchronized (budget) {
            return rejectRequestGhostHits;
        }
    }

    public long evictionGhostHits() {
        synchronized (budget) {
            return evictionGhostHits;
        }
    }

    public int lastVictimCount() {
        synchronized (budget) {
            return lastVictimCount;
        }
    }

    public void noteOriginalRequest(long requestBytes, boolean admitted) {
        if (requestBytes <= 0L) {
            return;
        }
        synchronized (budget) {
            requestSizes.note(requestBytes, admitted);
        }
    }

    public void noteOriginHit(long originBytes, long usefulBytes) {
        if (originBytes <= 0L || usefulBytes <= 0L) {
            return;
        }
        synchronized (budget) {
            requestSizes.hit(originBytes, usefulBytes);
        }
    }

    public long originRequestBytes(String objectId, long start) {
        synchronized (budget) {
            EntryMeta meta = metas.get(new BlockKey(objectId, start));
            return meta == null ? 0L : meta.originBytes;
        }
    }

    public long requestObserved(long requestBytes) {
        synchronized (budget) {
            return requestSizes.observed(requestBytes);
        }
    }

    public long requestAdmitted(long requestBytes) {
        synchronized (budget) {
            return requestSizes.admitted(requestBytes);
        }
    }

    public long requestRejected(long requestBytes) {
        synchronized (budget) {
            return requestSizes.rejected(requestBytes);
        }
    }

    public long requestResident(long requestBytes) {
        synchronized (budget) {
            return requestSizes.resident(requestBytes);
        }
    }

    public long requestHits(long requestBytes) {
        synchronized (budget) {
            return requestSizes.hits(requestBytes);
        }
    }

    public long requestUseful(long requestBytes) {
        synchronized (budget) {
            return requestSizes.useful(requestBytes);
        }
    }

    public String requestSizeFragment() {
        synchronized (budget) {
            return requestSizes.fragment();
        }
    }

    public long windowBytes() {
        synchronized (budget) {
            return windowBytes;
        }
    }

    public long probationBytes() {
        synchronized (budget) {
            return probationBytes;
        }
    }

    public long protectedBytes() {
        synchronized (budget) {
            return protectedBytes;
        }
    }

    public long sketchBytes() {
        synchronized (budget) {
            return policy == ReplacementPolicy.WTINYLFU
                   ? sketchBytes + (rejectGhosts.size() + evictionGhosts.size()) * 48L : 0L;
        }
    }

    public String remoteCostSnapshotFragment() {
        return remoteCosts.snapshotFragment();
    }

    public int blockCount() {
        synchronized (budget) {
            return blocks.size();
        }
    }

    private void unindex(String objectId, long start) {
        NavigableMap<Long, CachedBlock> starts = byObject.get(objectId);
        if (starts == null) {
            return;
        }
        starts.remove(start);
        if (starts.isEmpty()) {
            byObject.remove(objectId);
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

    private enum Segment {
        WINDOW,
        PROBATION,
        PROTECTED
    }

    private static final class EntryMeta {
        private final long bytes;
        private final long originBytes;
        private Segment segment;

        private EntryMeta(long bytes, long originBytes, Segment segment) {
            this.bytes = bytes;
            this.originBytes = originBytes;
            this.segment = segment;
        }
    }

    private static final class GhostMeta {
        private final long bytes;
        private final String reason;

        private GhostMeta(long bytes, String reason) {
            this.bytes = bytes;
            this.reason = reason;
        }
    }

    private static final class Piece {
        private final long start;
        private final int offset;
        private final int length;

        private Piece(long start, int offset, int length) {
            this.start = start;
            this.offset = offset;
            this.length = length;
        }
    }

    public static final class PinnedRange implements AutoCloseable {
        private final AppCache owner;
        private final List<CachedBlock> blocks;
        private final long start;
        private final long end;
        private boolean closed;

        private PinnedRange(AppCache owner, List<CachedBlock> blocks, long start, long end) {
            this.owner = owner;
            this.blocks = blocks;
            this.start = start;
            this.end = end;
        }

        public List<CachedBlock> blocks() {
            return blocks;
        }

        public long start() {
            return start;
        }

        public long end() {
            return end;
        }

        @Override
        public synchronized void close() {
            if (!closed) {
                closed = true;
                owner.releasePins(blocks);
            }
        }
    }

    private static final class RequestSizeHistogram {
        private static final long[] LIMITS = {
            4L << 10, 16L << 10, 64L << 10, 128L << 10, 256L << 10, 512L << 10,
            1L << 20, 2L << 20, 4L << 20, 8L << 20
        };
        private final long[] observed = new long[LIMITS.length + 1];
        private final long[] admitted = new long[LIMITS.length + 1];
        private final long[] rejected = new long[LIMITS.length + 1];
        private final long[] resident = new long[LIMITS.length + 1];
        private final long[] hits = new long[LIMITS.length + 1];
        private final long[] useful = new long[LIMITS.length + 1];

        private void note(long bytes, boolean admit) {
            int bucket = bucket(bytes);
            observed[bucket]++;
            if (admit) {
                admitted[bucket]++;
            } else {
                rejected[bucket]++;
            }
        }

        private void addResident(long bytes, long delta) {
            int bucket = bucket(bytes);
            resident[bucket] += delta;
            if (resident[bucket] < 0L) {
                resident[bucket] = 0L;
            }
        }

        private void hit(long bytes, long usefulBytes) {
            int bucket = bucket(bytes);
            hits[bucket]++;
            useful[bucket] += usefulBytes;
        }

        private long observed(long bytes) {
            return observed[bucket(bytes)];
        }

        private long admitted(long bytes) {
            return admitted[bucket(bytes)];
        }

        private long rejected(long bytes) {
            return rejected[bucket(bytes)];
        }

        private long resident(long bytes) {
            return resident[bucket(bytes)];
        }

        private long hits(long bytes) {
            return hits[bucket(bytes)];
        }

        private long useful(long bytes) {
            return useful[bucket(bytes)];
        }

        private String fragment() {
            StringBuilder sb = new StringBuilder("\"d1_request_sizes\":[");
            for (int i = 0; i < observed.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                long le = i < LIMITS.length ? LIMITS[i] : -1L;
                sb.append("{\"le\":").append(le)
                  .append(",\"observed\":").append(observed[i])
                  .append(",\"admitted\":").append(admitted[i])
                  .append(",\"rejected\":").append(rejected[i])
                  .append(",\"resident\":").append(resident[i])
                  .append(",\"hits\":").append(hits[i])
                  .append(",\"useful\":").append(useful[i])
                  .append('}');
            }
            return sb.append(']').toString();
        }

        private static int bucket(long bytes) {
            for (int i = 0; i < LIMITS.length; i++) {
                if (bytes <= LIMITS[i]) {
                    return i;
                }
            }
            return LIMITS.length;
        }
    }
}
