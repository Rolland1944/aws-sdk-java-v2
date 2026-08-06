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

import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Process-level, work-conserving byte budget shared by every app (query engine) in the JVM. There is a global hard
 * ceiling {@code capacity}; each registered app has a guaranteed {@code reserved} floor it can always cache into, and
 * may borrow idle capacity beyond that. When space is needed the budget reclaims <b>borrowed</b> bytes (usage above
 * an app's reserve) from other apps first, evicting their LRU blocks through the registered {@link CacheEvictor}; only
 * then does it evict the requester's own LRU. This guarantees the key isolation invariant: <b>an app is never forced
 * below its reserve by another app's activity</b>.
 *
 * <p>All mutating operations are serialized on this instance's monitor. Per-app caches synchronize on the same
 * monitor for their own mutations, so budget accounting and cache contents stay consistent and eviction call-backs
 * are re-entrant (single global lock; adequate for the moderate concurrency of a few engines).
 */
@SdkInternalApi
public final class GlobalBudget {

    private final long capacity;
    private final Map<String, Long> usage = new HashMap<>();
    private final Map<String, Long> reserved = new HashMap<>();
    private final Map<String, CacheEvictor> evictors = new HashMap<>();

    public GlobalBudget(long capacity) {
        this.capacity = Math.max(0L, capacity);
    }

    public long capacity() {
        return capacity;
    }

    /**
     * Register (or update) an app with its reserved floor and the cache that can be evicted on its behalf.
     */
    public synchronized void register(String appId, long reservedBytes, CacheEvictor evictor) {
        usage.putIfAbsent(appId, 0L);
        reserved.put(appId, Math.max(0L, reservedBytes));
        evictors.put(appId, evictor);
    }

    public synchronized long usage(String appId) {
        return usage.getOrDefault(appId, 0L);
    }

    public synchronized long reserved(String appId) {
        return reserved.getOrDefault(appId, 0L);
    }

    public synchronized long totalUsage() {
        long total = 0L;
        for (long v : usage.values()) {
            total += v;
        }
        return total;
    }

    /**
     * Try to reserve {@code bytes} of budget for {@code appId}, reclaiming borrowed space from other apps (then, if
     * still needed, from the requester's own LRU) as described in the class docs.
     *
     * @return {@code true} if the bytes were granted (and accounted); {@code false} if they could not fit even after
     *         reclamation (e.g. a single block larger than capacity).
     */
    public synchronized boolean acquire(String appId, long bytes) {
        if (bytes <= 0) {
            return true;
        }
        if (bytes > capacity) {
            return false;
        }
        if (capacity - totalUsage() >= bytes) {
            addUsage(appId, bytes);
            return true;
        }
        long deficit = bytes - (capacity - totalUsage());
        // Phase 1: reclaim borrowed bytes (usage above reserve) from OTHER apps, protecting their reserved floor.
        for (Map.Entry<String, Long> entry : usage.entrySet()) {
            String other = entry.getKey();
            if (other.equals(appId)) {
                continue;
            }
            long over = entry.getValue() - reserved.getOrDefault(other, 0L);
            if (over <= 0) {
                continue;
            }
            deficit -= reclaim(other, Math.min(over, deficit));
            if (deficit <= 0) {
                break;
            }
        }
        if (capacity - totalUsage() >= bytes) {
            addUsage(appId, bytes);
            return true;
        }
        // Phase 2: still short -> evict the requester's own LRU (self-inflicted growth beyond free capacity).
        long need = bytes - (capacity - totalUsage());
        if (need > 0) {
            reclaim(appId, need);
        }
        if (capacity - totalUsage() >= bytes) {
            addUsage(appId, bytes);
            return true;
        }
        return false;
    }

    public synchronized void release(String appId, long bytes) {
        addUsage(appId, -bytes);
    }

    private long reclaim(String appId, long bytes) {
        CacheEvictor evictor = evictors.get(appId);
        if (evictor == null || bytes <= 0) {
            return 0L;
        }
        long freed = evictor.evictLru(bytes);
        addUsage(appId, -freed);
        return freed;
    }

    private void addUsage(String appId, long delta) {
        long updated = usage.getOrDefault(appId, 0L) + delta;
        usage.put(appId, updated < 0 ? 0L : updated);
    }
}
