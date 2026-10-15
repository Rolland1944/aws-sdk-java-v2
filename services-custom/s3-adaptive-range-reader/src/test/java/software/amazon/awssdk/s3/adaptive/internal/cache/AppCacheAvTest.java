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

/**
 * W-TinyLFU with Aggregated Victims as published (Einziger et al., ACM TOS 2022). Capacity 1000 and 100-byte blocks
 * give a 100-byte window (1% rule floor) and a 900-byte main cache; sample size S = 10 x C = 100 accesses.
 */
class AppCacheAvTest {
    private static final long BLOCK = 100L;

    private static AppCache cache(long capacity) {
        GlobalBudget budget = new GlobalBudget(capacity);
        AppCache cache = new AppCache(new AppBudgetLease(budget, "app"));
        cache.setReplacementPolicy(ReplacementPolicy.WTINYLFU_AV);
        budget.register("app", capacity, cache);
        return cache;
    }

    private static boolean put(AppCache cache, long start, int length) {
        return cache.putRequest("o", start, new byte[length], length);
    }

    private static boolean put(AppCache cache, long block) {
        return put(cache, block * BLOCK, (int) BLOCK);
    }

    private static void hit(AppCache cache, long block, int times) {
        for (int i = 0; i < times; i++) {
            assertThat(cache.findCovering("o", block * BLOCK, (block + 1) * BLOCK)).isNotNull();
        }
    }

    private static boolean cached(AppCache cache, long block) {
        return cache.findCovering("o", block * BLOCK, (block + 1) * BLOCK) != null;
    }

    private static void fill(AppCache cache) {
        for (long b = 0; b < 10; b++) {
            assertThat(put(cache, b)).isTrue();
        }
    }

    @Test
    void newItemsAlwaysEnterTheWindowAndOverflowMovesToMain() {
        AppCache cache = cache(1000);
        put(cache, 0);
        assertThat(cache.windowBytes()).isEqualTo(100);
        put(cache, 1);
        put(cache, 2);

        assertThat(cache.windowBytes()).isEqualTo(100);
        assertThat(cache.probationBytes()).isEqualTo(200);
        assertThat(cache.evictedBlocks()).isZero();
        assertThat(cache.rejectedBlocks()).isZero();
    }

    @Test
    void neverExceedsCapacity() {
        AppCache cache = cache(1000);
        for (long b = 0; b < 60; b++) {
            put(cache, b);
            assertThat(cache.cachedBytes()).isLessThanOrEqualTo(1000);
        }
        assertThat(cache.blockCount()).isLessThanOrEqualTo(10);
    }

    @Test
    void candidateBelowVictimFrequencyIsRejectedAndDropped() {
        AppCache cache = cache(1000);
        fill(cache);              // window b9, main b0..b8
        for (long b = 0; b < 9; b++) {
            hit(cache, b, 3);     // main entries: frequency 4
        }
        long evictedBefore = cache.evictedBlocks();

        assertThat(put(cache, 10)).isTrue(); // b10 enters the window, b9 is the candidate (frequency 1)

        assertThat(cache.avRejectedCandidates()).isEqualTo(1);
        assertThat(cache.evictedBlocks()).isEqualTo(evictedBefore + 1);
        assertThat(cache.cachedBytes()).isEqualTo(1000);
        assertThat(cached(cache, 9)).isFalse();
        for (long b = 0; b < 9; b++) {
            assertThat(cached(cache, b)).isTrue();
        }
    }

    @Test
    void frequentCandidateReplacesTheLeastRecentMainVictim() {
        AppCache cache = cache(1000);
        fill(cache);
        hit(cache, 9, 5);         // window hit stays in the window, frequency 6
        assertThat(cache.windowBytes()).isEqualTo(100);
        assertThat(cache.protectedBytes()).isZero();

        put(cache, 10);           // b9 is the candidate, main victim is b0 (frequency 1)

        assertThat(cache.evictedBlocks()).isEqualTo(1);
        assertThat(cache.probationBytes()).isEqualTo(900);
        assertThat(cache.windowBytes()).isEqualTo(100);
        assertThat(cached(cache, 0)).isFalse();
        assertThat(cached(cache, 9)).isTrue();
        assertThat(cached(cache, 10)).isTrue();
    }

    @Test
    void candidateMustOutweighTheSumOfAllVictimsNotEachOne() {
        AppCache cache = cache(1000);
        fill(cache);
        for (long b = 0; b < 9; b++) {
            hit(cache, b, 1);     // main entries: frequency 2
        }
        // A 250-byte item must displace at least two 100-byte victims (summed frequency 4).
        // Each attempt counts as an access even though it is rejected, so its frequency rises 1, 2, 3, 4.
        assertThat(put(cache, 5000, 250)).isFalse();
        assertThat(put(cache, 5000, 250)).isFalse();
        assertThat(put(cache, 5000, 250)).isFalse(); // frequency 3 > 2, but 3 < 2 + 2
        assertThat(cache.findCovering("o", 5000, 5250)).isNull();

        long evictedBefore = cache.evictedBlocks();
        assertThat(put(cache, 5000, 250)).isTrue();  // frequency 4 >= 4
        assertThat(cache.evictedBlocks()).isGreaterThanOrEqualTo(evictedBefore + 2);
        assertThat(cache.findCovering("o", 5000, 5250)).isNotNull();
        assertThat(cache.cachedBytes()).isLessThanOrEqualTo(1000);
    }

    @Test
    void rejectionPromotesTheVictimsSoTheNextCandidateSeesDifferentOnes() {
        AppCache cache = cache(1000);
        fill(cache);
        hit(cache, 0, 1);         // b0 frequency 2 -> protected
        // b1..b8 stay in probation with frequency 1; b0 is protected.
        // First candidate (frequency 1) vs victim b1 (frequency 1): admitted (>=), b1 evicted.
        put(cache, 10);
        assertThat(cached(cache, 1)).isFalse();
        // Make every probation entry look hot so the next candidate is rejected, and the victim is promoted.
        for (long b = 2; b <= 8; b++) {
            hit(cache, b, 2);
        }
        long protectedBefore = cache.protectedBytes();
        long rejectedBefore = cache.avRejectedCandidates();
        put(cache, 11);
        put(cache, 12);
        assertThat(cache.avRejectedCandidates()).isGreaterThan(rejectedBefore);
        assertThat(cache.protectedBytes()).isGreaterThanOrEqualTo(Math.min(protectedBefore, 720L));
    }

    @Test
    void pinnedBlocksAreNeverVictims() {
        AppCache cache = cache(1000);
        fill(cache);
        AppCache.PinnedRange pin = cache.pinCoveringSpan("o", 0, BLOCK);
        assertThat(pin).isNotNull();
        for (long b = 10; b < 80; b++) {
            put(cache, b);
        }
        assertThat(cache.cachedBytes()).isLessThanOrEqualTo(1000);
        assertThat(cached(cache, 0)).isTrue();
        pin.close();
    }

    @Test
    void countersAreHalvedEverySampleSizeAccessesAndCapped() {
        AppCache cache = cache(1000);
        put(cache, 0);
        assertThat(cache.avResets()).isZero();
        hit(cache, 0, 120);       // S = 100 accesses, so at least one reset
        assertThat(cache.avResets()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void lowerSoftCapacityEvictsThroughTheSegments() {
        AppCache cache = cache(1000);
        fill(cache);
        cache.setSoftCapacity(500);
        assertThat(cache.cachedBytes()).isLessThanOrEqualTo(500);
        assertThat(cache.windowBytes() + cache.probationBytes() + cache.protectedBytes())
            .isEqualTo(cache.cachedBytes());
    }

    @Test
    void itemLargerThanCapacityIsRejected() {
        AppCache cache = cache(1000);
        assertThat(put(cache, 0, 2000)).isFalse();
        assertThat(cache.cachedBytes()).isZero();
    }

    @Test
    void segmentBytesAlwaysMatchResidentBytes() {
        AppCache cache = cache(1000);
        for (long b = 0; b < 200; b++) {
            put(cache, b % 37);
            if (b % 3 == 0) {
                cache.findCovering("o", (b % 11) * BLOCK, (b % 11 + 1) * BLOCK);
            }
            assertThat(cache.windowBytes() + cache.probationBytes() + cache.protectedBytes())
                .isEqualTo(cache.cachedBytes());
            assertThat(cache.cachedBytes()).isLessThanOrEqualTo(1000);
        }
    }
}
