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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import software.amazon.awssdk.s3.adaptive.internal.budget.AppBudgetLease;
import software.amazon.awssdk.s3.adaptive.internal.budget.GlobalBudget;
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache.ReplacementPolicy;

/** GDSF and SIEVE wired into {@link AppCache}: the baselines compared against the W-TinyLFU path. */
class AppCachePolicyTest {

    private static byte[] bytes(int n) {
        return new byte[n];
    }

    private static AppCache cache(long capacity, ReplacementPolicy policy) {
        GlobalBudget budget = new GlobalBudget(capacity);
        AppBudgetLease lease = new AppBudgetLease(budget, "app");
        AppCache cache = new AppCache(lease);
        cache.setReplacementPolicy(policy);
        budget.register("app", capacity, cache);
        return cache;
    }

    private static boolean has(AppCache cache, long start, long end) {
        return cache.findCovering("o", start, end) != null;
    }

    @ParameterizedTest
    @EnumSource(value = ReplacementPolicy.class, names = {"GDSF", "GDSF_UNIT", "SIEVE"})
    void neverExceedsCapacityAndAlwaysAdmits(ReplacementPolicy policy) {
        AppCache cache = cache(300, policy);
        for (int i = 0; i < 20; i++) {
            assertThat(cache.put("o", i * 100L, bytes(100))).isTrue();
            assertThat(cache.cachedBytes()).isLessThanOrEqualTo(300);
        }
        assertThat(cache.cachedBytes()).isEqualTo(300);
        assertThat(cache.blockCount()).isEqualTo(3);
        assertThat(cache.evictedBlocks()).isEqualTo(17);
        assertThat(cache.rejectedBlocks()).isZero();
        assertThat(cache.replacementPolicy()).isEqualTo(policy);
    }

    @ParameterizedTest
    @EnumSource(value = ReplacementPolicy.class, names = {"GDSF", "GDSF_UNIT", "SIEVE"})
    void blockLargerThanCapacityIsNotCached(ReplacementPolicy policy) {
        AppCache cache = cache(100, policy);
        assertThat(cache.put("o", 0, bytes(200))).isFalse();
        assertThat(cache.cachedBytes()).isZero();
    }

    @ParameterizedTest
    @EnumSource(value = ReplacementPolicy.class, names = {"GDSF", "GDSF_UNIT", "SIEVE"})
    void hotBlockSurvivesScanChurn(ReplacementPolicy policy) {
        AppCache cache = cache(300, policy);
        cache.put("o", 0, bytes(100));
        for (int i = 1; i < 30; i++) {
            assertThat(has(cache, 0, 100)).isTrue(); // keep touching the hot block
            cache.put("o", i * 100L, bytes(100));
        }
        assertThat(has(cache, 0, 100)).isTrue();
    }

    @ParameterizedTest
    @EnumSource(value = ReplacementPolicy.class, names = {"GDSF", "GDSF_UNIT", "SIEVE"})
    void pinnedBlockIsNeverEvicted(ReplacementPolicy policy) {
        AppCache cache = cache(300, policy);
        cache.put("o", 0, bytes(100));
        cache.put("o", 100, bytes(100));
        cache.put("o", 200, bytes(100));
        AppCache.PinnedRange pin = cache.pinCoveringSpan("o", 0, 100);
        assertThat(pin).isNotNull();
        for (int i = 3; i < 15; i++) {
            cache.put("o", i * 100L, bytes(100));
        }
        assertThat(cache.cachedBytes()).isEqualTo(300);
        assertThat(has(cache, 0, 100)).isTrue();
        pin.close();
    }

    @ParameterizedTest
    @EnumSource(value = ReplacementPolicy.class, names = {"GDSF", "GDSF_UNIT", "SIEVE"})
    void allPinnedRejectsInsteadOfOverflowing(ReplacementPolicy policy) {
        AppCache cache = cache(200, policy);
        cache.put("o", 0, bytes(100));
        cache.put("o", 100, bytes(100));
        AppCache.PinnedRange a = cache.pinCoveringSpan("o", 0, 100);
        AppCache.PinnedRange b = cache.pinCoveringSpan("o", 100, 200);

        assertThat(cache.putRequest("o", 200, bytes(100), 100)).isFalse();
        assertThat(cache.cachedBytes()).isEqualTo(200);
        a.close();
        b.close();
        assertThat(cache.putRequest("o", 200, bytes(100), 100)).isTrue();
        assertThat(cache.cachedBytes()).isEqualTo(200);
    }

    @ParameterizedTest
    @EnumSource(value = ReplacementPolicy.class, names = {"GDSF", "GDSF_UNIT", "SIEVE"})
    void multiBlockRequestEvictsEnoughAndInsertsAllPieces(ReplacementPolicy policy) {
        AppCache cache = cache(300, policy);
        cache.put("o", 0, bytes(100));
        cache.put("o", 100, bytes(100));
        cache.put("o", 200, bytes(100));

        assertThat(cache.putRequest("o", 1000, bytes(250), 100)).isTrue(); // 100 + 100 + 50
        assertThat(cache.cachedBytes()).isEqualTo(250);
        assertThat(has(cache, 1000, 1100)).isTrue();
        assertThat(has(cache, 1100, 1200)).isTrue();
        assertThat(has(cache, 1200, 1250)).isTrue();
        assertThat(cache.evictedBlocks()).isEqualTo(3);
    }

    @ParameterizedTest
    @EnumSource(value = ReplacementPolicy.class, names = {"GDSF", "GDSF_UNIT", "SIEVE"})
    void reinsertingSameStartReplacesBytes(ReplacementPolicy policy) {
        AppCache cache = cache(300, policy);
        cache.put("o", 0, bytes(100));
        cache.put("o", 0, bytes(150));
        assertThat(cache.cachedBytes()).isEqualTo(150);
        assertThat(cache.blockCount()).isEqualTo(1);
        assertThat(cache.evictedBlocks()).isZero();
    }

    @ParameterizedTest
    @EnumSource(value = ReplacementPolicy.class, names = {"GDSF", "GDSF_UNIT", "SIEVE"})
    void softCapacityAndBudgetEvictionUseThePolicy(ReplacementPolicy policy) {
        AppCache cache = cache(400, policy);
        for (int i = 0; i < 4; i++) {
            cache.put("o", i * 100L, bytes(100));
        }
        cache.setSoftCapacity(200);
        assertThat(cache.cachedBytes()).isEqualTo(200);

        long freed = cache.evictLru(100);
        assertThat(freed).isEqualTo(100);
        assertThat(cache.cachedBytes()).isEqualTo(100);
        cache.setSoftCapacity(0);
        assertThat(cache.cachedBytes()).isZero();
        assertThat(cache.blockCount()).isZero();
    }

    @Test
    void sieveKeepsVisitedBlockAndEvictsOldestUnvisited() {
        AppCache cache = cache(300, ReplacementPolicy.SIEVE);
        cache.put("o", 0, bytes(100));   // A
        cache.put("o", 100, bytes(100)); // B
        cache.put("o", 200, bytes(100)); // C
        assertThat(has(cache, 0, 100)).isTrue(); // visit A

        cache.put("o", 300, bytes(100)); // D evicts B, not A
        assertThat(has(cache, 100, 200)).isFalse();
        assertThat(has(cache, 0, 100)).isTrue();
        assertThat(has(cache, 200, 300)).isTrue();
        assertThat(has(cache, 300, 400)).isTrue();
    }

    @Test
    void gdsfUnitPrefersEvictingLargeBlocks() {
        AppCache cache = cache(300, ReplacementPolicy.GDSF_UNIT);
        cache.put("o", 0, bytes(50));     // small: priority 1/50
        cache.put("o", 1000, bytes(250)); // large: priority 1/250

        assertThat(cache.put("o", 2000, bytes(100))).isTrue();
        assertThat(has(cache, 1000, 1250)).isFalse();
        assertThat(has(cache, 0, 50)).isTrue();
        assertThat(has(cache, 2000, 2100)).isTrue();
        assertThat(cache.cachedBytes()).isEqualTo(150);
    }

    @Test
    void gdsfUnitFrequencyProtectsAgainstOlderPeers() {
        AppCache cache = cache(300, ReplacementPolicy.GDSF_UNIT);
        cache.put("o", 0, bytes(100));
        cache.put("o", 100, bytes(100));
        cache.put("o", 200, bytes(100));
        assertThat(has(cache, 100, 200)).isTrue();
        assertThat(has(cache, 100, 200)).isTrue(); // B has the highest priority

        cache.put("o", 300, bytes(100));           // evicts A (lowest, oldest)
        assertThat(has(cache, 0, 100)).isFalse();
        assertThat(has(cache, 100, 200)).isTrue();
    }

    @Test
    void switchingPolicyIndexesExistingBlocks() {
        GlobalBudget budget = new GlobalBudget(300);
        AppCache cache = new AppCache(new AppBudgetLease(budget, "app"));
        budget.register("app", 300, cache);
        cache.put("o", 0, bytes(100));
        cache.put("o", 100, bytes(100));
        cache.put("o", 200, bytes(100));

        cache.setReplacementPolicy(ReplacementPolicy.SIEVE);
        cache.put("o", 300, bytes(100));

        assertThat(cache.cachedBytes()).isEqualTo(300);
        assertThat(cache.evictedBlocks()).isEqualTo(1);
    }
}
