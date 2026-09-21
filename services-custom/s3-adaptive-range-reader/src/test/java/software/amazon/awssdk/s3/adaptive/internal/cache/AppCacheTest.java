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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.internal.budget.AppBudgetLease;
import software.amazon.awssdk.s3.adaptive.internal.budget.GlobalBudget;

class AppCacheTest {

    private static byte[] bytes(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) {
            b[i] = (byte) i;
        }
        return b;
    }

    private static AppCache cache(long capacity) {
        GlobalBudget budget = new GlobalBudget(capacity);
        AppBudgetLease lease = new AppBudgetLease(budget, "app");
        AppCache cache = new AppCache(lease);
        budget.register("app", capacity, cache);
        return cache;
    }

    @Test
    void coveringHitFromAnEarlierStartingBlock() {
        AppCache cache = cache(1024);
        cache.put("o", 100, bytes(200)); // [100, 300)

        assertThat(cache.findCovering("o", 100, 300)).isNotNull();
        assertThat(cache.findCovering("o", 120, 180)).isNotNull();
        assertThat(cache.findCovering("o", 299, 300)).isNotNull();
        // partial overlap is a miss (no cross-block stitching)
        assertThat(cache.findCovering("o", 250, 350)).isNull();
        assertThat(cache.findCovering("o", 0, 50)).isNull();
        // other objects do not bleed through
        assertThat(cache.findCovering("other", 120, 180)).isNull();
    }

    @Test
    void tightestCoveringBlockWinsWhenBlocksOverlap() {
        AppCache cache = cache(1024);
        cache.put("o", 0, bytes(400));   // [0, 400)
        cache.put("o", 100, bytes(100)); // [100, 200)

        CachedBlock hit = cache.findCovering("o", 120, 180);
        assertThat(hit).isNotNull();
        assertThat(hit.start()).isEqualTo(100);
        // only the wider block can serve this one
        assertThat(cache.findCovering("o", 120, 350).start()).isEqualTo(0);
    }

    @Test
    void evictedBlocksStopServingHits() {
        AppCache cache = cache(300);
        cache.put("o", 0, bytes(100));   // A
        cache.put("o", 100, bytes(100)); // B
        cache.put("o", 200, bytes(100)); // C -> full

        assertThat(cache.findCovering("o", 0, 100)).isNotNull(); // touch A

        cache.put("o", 300, bytes(100)); // evicts LRU (B)
        assertThat(cache.cachedBytes()).isEqualTo(300);
        assertThat(cache.evictedBytes()).isEqualTo(100);
        assertThat(cache.evictedBlocks()).isEqualTo(1);
        assertThat(cache.blockCount()).isEqualTo(3);
        assertThat(cache.findCovering("o", 100, 200)).isNull();
        assertThat(cache.findCovering("o", 0, 100)).isNotNull();
        assertThat(cache.findCovering("o", 300, 400)).isNotNull();
    }

    @Test
    void reinsertingSameStartReplacesBytes() {
        AppCache cache = cache(1024);
        cache.put("o", 0, bytes(50));
        cache.put("o", 0, bytes(80));
        assertThat(cache.cachedBytes()).isEqualTo(80);
        assertThat(cache.blockCount()).isEqualTo(1);
        assertThat(cache.findCovering("o", 0, 80)).isNotNull();
    }

    @Test
    void softCapacityEvictsWithoutRebuild() {
        AppCache cache = cache(1000);
        cache.put("o", 0, bytes(200));
        cache.put("o", 200, bytes(200));
        cache.put("o", 400, bytes(200));
        assertThat(cache.cachedBytes()).isEqualTo(600);

        cache.setSoftCapacity(250);
        assertThat(cache.softCapacity()).isEqualTo(250);
        assertThat(cache.cachedBytes()).isLessThanOrEqualTo(250);
        assertThat(cache.findCovering("o", 400, 600)).isNotNull();
        assertThat(cache.findCovering("o", 0, 200)).isNull();
    }

    @Test
    void softZeroStopsNewPutsAndDrains() {
        AppCache cache = cache(1000);
        cache.put("o", 0, bytes(100));
        cache.setSoftCapacity(0);
        assertThat(cache.cachedBytes()).isEqualTo(0);
        assertThat(cache.put("o", 100, bytes(50))).isFalse();
        assertThat(cache.findCovering("o", 0, 100)).isNull();
    }

    @Test
    void raisingSoftDoesNotEvict() {
        AppCache cache = cache(1000);
        cache.setSoftCapacity(200);
        cache.put("o", 0, bytes(100));
        cache.setSoftCapacity(800);
        assertThat(cache.findCovering("o", 0, 100)).isNotNull();
        assertThat(cache.put("o", 200, bytes(200))).isTrue();
        assertThat(cache.cachedBytes()).isEqualTo(300);
    }

    @Test
    void blockLargerThanCapacityIsNotCached() {
        AppCache cache = cache(100);
        assertThat(cache.put("o", 0, bytes(200))).isFalse();
        assertThat(cache.cachedBytes()).isEqualTo(0);
        assertThat(cache.findCovering("o", 0, 200)).isNull();
    }
}
