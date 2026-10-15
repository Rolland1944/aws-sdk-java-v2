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
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache.ReplacementPolicy;

/** Segmented LRU: probation on insert, protected on hit, protected capped at 80% of capacity. */
class AppCacheSlruTest {
    private static final long BLOCK = 100L;

    private static AppCache cache(long capacity) {
        GlobalBudget budget = new GlobalBudget(capacity);
        AppCache cache = new AppCache(new AppBudgetLease(budget, "app"));
        cache.setReplacementPolicy(ReplacementPolicy.SLRU);
        budget.register("app", capacity, cache);
        return cache;
    }

    private static boolean put(AppCache cache, long block) {
        return cache.putRequest("o", block * BLOCK, new byte[(int) BLOCK], BLOCK);
    }

    private static void hit(AppCache cache, long block) {
        assertThat(cache.findCovering("o", block * BLOCK, (block + 1) * BLOCK)).isNotNull();
    }

    private static boolean cached(AppCache cache, long block) {
        return cache.findCovering("o", block * BLOCK, (block + 1) * BLOCK) != null;
    }

    @Test
    void newBlocksEnterProbationUntilHit() {
        AppCache cache = cache(1000);
        for (long b = 0; b < 10; b++) {
            assertThat(put(cache, b)).isTrue();
        }
        assertThat(cache.windowBytes()).isZero();
        assertThat(cache.probationBytes()).isEqualTo(1000);
        assertThat(cache.protectedBytes()).isZero();
        assertThat(cache.rejectedBlocks()).isZero();
    }

    @Test
    void hitPromotesAndProtectedStaysWithinEightyPercent() {
        AppCache cache = cache(1000);
        for (long b = 0; b < 10; b++) {
            put(cache, b);
            hit(cache, b);
        }
        assertThat(cache.protectedBytes()).isEqualTo(800);
        assertThat(cache.probationBytes()).isEqualTo(200);
        assertThat(cache.cachedBytes()).isEqualTo(1000);
    }

    @Test
    void promotedBlockSurvivesProbationChurn() {
        AppCache cache = cache(1000);
        for (long b = 0; b < 10; b++) {
            put(cache, b);
        }
        hit(cache, 0);
        for (long b = 10; b < 40; b++) {
            put(cache, b);
            hit(cache, 0);
        }
        assertThat(cache.cachedBytes()).isEqualTo(1000);
        assertThat(cached(cache, 0)).isTrue();
        assertThat(cache.evictedBlocks()).isGreaterThan(0);
    }

    @Test
    void probationIsEvictedBeforeProtected() {
        AppCache cache = cache(1000);
        for (long b = 0; b < 10; b++) {
            put(cache, b);
        }
        for (long b = 1; b < 10; b++) {
            hit(cache, b); // b0 stays the oldest probation entry
        }
        put(cache, 10);
        assertThat(cached(cache, 0)).isFalse();
        assertThat(cached(cache, 1)).isTrue();
        assertThat(cache.cachedBytes()).isEqualTo(1000);
    }

    @Test
    void pinnedBlockIsNeverEvicted() {
        AppCache cache = cache(1000);
        for (long b = 0; b < 10; b++) {
            put(cache, b);
        }
        AppCache.PinnedRange pin = cache.pinCoveringSpan("o", 0, BLOCK);
        assertThat(pin).isNotNull();
        for (long b = 10; b < 50; b++) {
            put(cache, b);
        }
        assertThat(cache.cachedBytes()).isEqualTo(1000);
        assertThat(cached(cache, 0)).isTrue();
        pin.close();
    }

    @Test
    void blockLargerThanCapacityIsRejected() {
        AppCache cache = cache(1000);
        assertThat(cache.putRequest("o", 0, new byte[2000], 2000)).isFalse();
        assertThat(cache.cachedBytes()).isZero();
    }
}
