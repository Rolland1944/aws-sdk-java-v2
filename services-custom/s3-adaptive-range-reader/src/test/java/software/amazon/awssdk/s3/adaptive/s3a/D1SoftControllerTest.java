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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.internal.RuntimeConfig;
import software.amazon.awssdk.s3.adaptive.internal.budget.AppBudgetLease;
import software.amazon.awssdk.s3.adaptive.internal.budget.GlobalBudget;
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache;

class D1SoftControllerTest {

    private static AppCache cache(long capacity) {
        GlobalBudget budget = new GlobalBudget(capacity);
        AppBudgetLease lease = new AppBudgetLease(budget, "app");
        AppCache cache = new AppCache(lease);
        budget.register("app", capacity, cache);
        return cache;
    }

    private static RuntimeConfig adaptive() {
        return RuntimeConfig.builder()
                            .d1Enabled(true)
                            .d1Adaptive(true)
                            .d1AdmitMaxBytes(256 * 1024)
                            .d1ObserveBudgetBytes(256 * 1024)
                            .d1HardCacheBytes(4 * 1024 * 1024)
                            .d1MinBudgetBytes(64 * 1024)
                            .d1TargetCoverage(0.5)
                            .d1ObserveGets(4)
                            .d1HeapHigh(0.70)
                            .d1HeapLow(0.50)
                            .build();
    }

    @Test
    void frozenConfigKeepsStaticAdmit() {
        RuntimeConfig cfg = RuntimeConfig.builder()
                                         .d1Enabled(true)
                                         .d1Adaptive(false)
                                         .d1AdmitMaxBytes(100)
                                         .build();
        D1SoftController c = new D1SoftController(cfg, cache(1024));
        assertThat(c.admit("o", 0, 50)).isTrue();
        assertThat(c.admit("o", 0, 200)).isFalse();
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.OBSERVE);
    }

    @Test
    void fixedAdmissionBoundsAdaptivePolicy() {
        RuntimeConfig cfg = RuntimeConfig.builder()
                                         .d1Enabled(true)
                                         .d1Adaptive(true)
                                         .d1FixedCapacity(true)
                                         .d1FixedAdmission(true)
                                         .d1AdmitMaxBytes(2 * 1024 * 1024)
                                         .d1ObserveBudgetBytes(2 * 1024 * 1024)
                                         .d1HardCacheBytes(2 * 1024 * 1024)
                                         .build();
        D1SoftController c = new D1SoftController(cfg, cache(2 * 1024 * 1024));
        assertThat(c.targetBudget()).isEqualTo(2 * 1024 * 1024);
        assertThat(c.admit("o", 0, 2 * 1024 * 1024)).isTrue();
        assertThat(c.admit("o", 0, 2 * 1024 * 1024 + 1)).isFalse();
    }

    @Test
    void noReuseAfterObserveWindowKeepsSoftTrackForPolicyLayer() {
        AppCache cache = cache(4 * 1024 * 1024);
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = new D1SoftController(adaptive(), cache, new WorkingSetWindow(32), heap::get);
        for (int i = 0; i < 8; i++) {
            c.observe("o", i * 1000L, 100);
        }
        c.tick();
        c.tick();
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.TRACK);
        assertThat(c.targetBudget()).isEqualTo(256 * 1024);
        assertThat(c.admit("o", 0, 100)).isTrue();
        assertThat(cache.softCapacity()).isEqualTo(256 * 1024);
    }

    @Test
    void reuseTracksCoverageWithoutClearingCache() {
        AppCache cache = cache(4 * 1024 * 1024);
        cache.put("o", 0, new byte[200]);
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = new D1SoftController(adaptive(), cache, new WorkingSetWindow(32), heap::get);
        for (int i = 0; i < 6; i++) {
            c.observe("hot", 0, 1024);
        }
        c.tick();
        c.tick();
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.TRACK);
        assertThat(c.targetBudget()).isGreaterThan(0);
        assertThat(cache.findCovering("o", 0, 200)).isNotNull();
        assertThat(c.admit("hot", 0, 1024)).isTrue();
    }

    @Test
    void heapHighWaterShrinksTarget() {
        AppCache cache = cache(4 * 1024 * 1024);
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = new D1SoftController(adaptive(), cache, new WorkingSetWindow(32), heap::get);
        for (int i = 0; i < 6; i++) {
            c.observe("hot", 0, 1024);
        }
        c.tick();
        c.tick();
        long before = c.targetBudget();
        heap.set(0.90);
        c.tick();
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.SHRINK);
        assertThat(c.targetBudget()).isLessThanOrEqualTo(Math.max(64 * 1024, before / 2));
        assertThat(c.shrinks()).isGreaterThan(0);
    }

    @Test
    void scansDoNotInflateBudgetButAdmissionIsPolicyDriven() {
        AppCache cache = cache(4 * 1024 * 1024);
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = new D1SoftController(adaptive(), cache, new WorkingSetWindow(64), heap::get);
        for (int i = 0; i < 10; i++) {
            c.observe("scan-" + i, 0, 4 * 1024 * 1024);
        }
        for (int i = 0; i < 6; i++) {
            c.observe("hot", 0, 1024);
        }
        for (int i = 0; i < 8; i++) {
            c.tick();
        }
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.TRACK);
        assertThat(c.budgetWindow().reusableBytes()).isEqualTo(1024);
        assertThat(c.admissionWindow().reusableBytes()).isEqualTo(1024);
        assertThat(c.admissionWindow().pollution()).isGreaterThan(0.9);
        assertThat(c.admitMax()).isEqualTo(4 * 1024 * 1024);
        assertThat(c.admit("scan-0", 0, 4 * 1024 * 1024)).isTrue();
        assertThat(c.admit("hot", 0, 1024)).isTrue();
    }

    @Test
    void spareBudgetAloneDoesNotGrowTargetButAdmissionUsesPhysicalLimit() {
        RuntimeConfig cfg = RuntimeConfig.builder()
                                         .d1Enabled(true)
                                         .d1Adaptive(true)
                                         .d1AdmitMaxBytes(256 * 1024)
                                         .d1ObserveBudgetBytes(256 * 1024)
                                         .d1HardCacheBytes(64 * 1024 * 1024)
                                         .d1MinBudgetBytes(64 * 1024)
                                         .d1TargetCoverage(1.0)
                                         .d1ObserveGets(4)
                                         .d1HeapHigh(0.70)
                                         .d1HeapLow(0.50)
                                         .build();
        AppCache cache = cache(64 * 1024 * 1024);
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = new D1SoftController(cfg, cache, new WorkingSetWindow(32), heap::get);
        for (int i = 0; i < 6; i++) {
            c.observe("hot", 0, 1024);
        }
        for (int i = 0; i < 8; i++) {
            c.tick();
        }
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.TRACK);
        assertThat(c.targetBudget()).isGreaterThan(0);
        assertThat(c.admitMax()).isEqualTo(8 * 1024 * 1024);
        assertThat(c.admit("mid", 0, 300 * 1024)).isTrue();
    }

    @Test
    void smallReusableSetDoesNotShrinkObserveBudget() {
        RuntimeConfig cfg = RuntimeConfig.builder()
                                         .d1Enabled(true)
                                         .d1Adaptive(true)
                                         .d1AdmitMaxBytes(256 * 1024)
                                         .d1ObserveBudgetBytes(256 * 1024 * 1024)
                                         .d1HardCacheBytes(4L * 1024 * 1024 * 1024)
                                         .d1MinBudgetBytes(16 * 1024 * 1024)
                                         .d1TargetCoverage(1.0)
                                         .d1ObserveGets(4)
                                         .d1HeapHigh(0.70)
                                         .d1HeapLow(0.50)
                                         .build();
        AppCache cache = cache(4L * 1024 * 1024 * 1024);
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = new D1SoftController(cfg, cache, new WorkingSetWindow(32), heap::get);
        for (int i = 0; i < 6; i++) {
            c.observe("hot", 0, 1024);
        }
        for (int i = 0; i < 8; i++) {
            c.tick();
        }
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.TRACK);
        assertThat(c.targetBudget()).isEqualTo(256L * 1024 * 1024);
        assertThat(c.admitMax()).isEqualTo(8L * 1024 * 1024);
    }

    @Test
    void rejectGhostDoesNotGrowTarget() {
        RuntimeConfig cfg = tinyAdaptive();
        AppCache cache = cache(4000);
        cache.setReplacementPolicy(AppCache.ReplacementPolicy.WTINYLFU);
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = new D1SoftController(cfg, cache, new WorkingSetWindow(32), heap::get);
        for (int i = 0; i < 8; i++) {
            c.observe("hot", 0, 16);
        }
        c.tick();
        c.tick();
        protectTwoAndRejectThird(cache);
        long target = c.targetBudget();
        long evictionHits = cache.evictionGhostHits();
        for (int i = 0; i < 6; i++) {
            cache.put("o", 400, new byte[100]);
            c.tick();
        }
        assertThat(cache.rejectGhostHits()).isGreaterThan(0);
        assertThat(cache.evictionGhostHits()).isEqualTo(evictionHits);
        assertThat(c.targetBudget()).isEqualTo(target);
        assertThat(c.snapshotFragment()).contains("reject-ghost-ignored");
    }

    @Test
    void evictionGhostGrowsOnlyAfterConsecutiveHorizons() {
        AppCache cache = cache(4 * 1024 * 1024);
        cache.setReplacementPolicy(AppCache.ReplacementPolicy.WTINYLFU);
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = new D1SoftController(adaptive(), cache, new WorkingSetWindow(32), heap::get);
        for (int i = 0; i < 8; i++) {
            c.observe("hot", 0, 1024);
        }
        for (int i = 0; i < 4; i++) {
            c.tick();
        }
        long target = c.targetBudget();
        int size = 64 * 1024;
        for (int i = 0; i < 4; i++) {
            assertThat(cache.put("o", i * (long) size, new byte[size])).isTrue();
        }
        assertThat(cache.put("o", 10L * size, new byte[size])).isTrue();
        assertThat(cache.evictedBlocks()).isEqualTo(1);
        assertThat(cache.put("o", 0, new byte[size])).isTrue();
        assertThat(cache.evictionGhostHits()).isEqualTo(1);
        c.tick();
        assertThat(c.targetBudget()).isEqualTo(target);
        assertThat(cache.put("o", size, new byte[size])).isTrue();
        assertThat(cache.evictionGhostHits()).isGreaterThan(1);
        c.tick();
        assertThat(c.targetBudget()).isGreaterThan(target);
        assertThat(c.snapshotFragment()).contains("eviction-ghost-grow");
    }

    @Test
    void heapShrinkCooldownIgnoresEvictionGhosts() {
        AppCache cache = cache(4 * 1024 * 1024);
        cache.setReplacementPolicy(AppCache.ReplacementPolicy.WTINYLFU);
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = new D1SoftController(adaptive(), cache, new WorkingSetWindow(32), heap::get);
        for (int i = 0; i < 8; i++) {
            c.observe("hot", 0, 1024);
        }
        c.tick();
        c.tick();
        heap.set(0.90);
        c.tick();
        long shrunk = c.targetBudget();
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.SHRINK);
        heap.set(0.2);
        int size = 64 * 1024;
        for (int i = 0; i < 4; i++) {
            cache.put("o", i * (long) size, new byte[size]);
        }
        cache.put("o", 10L * size, new byte[size]);
        cache.put("o", 0, new byte[size]);
        cache.put("o", size, new byte[size]);
        for (int i = 0; i < 6; i++) {
            c.tick();
            assertThat(c.targetBudget()).isLessThanOrEqualTo(shrunk);
        }
        assertThat(cache.evictionGhostHits()).isGreaterThan(0);
        assertThat(c.snapshotFragment()).contains("cooldown");
    }

    @Test
    void repeatedWholeRequestRejectCanProbeOneCapacityStep() {
        AppCache cache = cache(4 * 1024 * 1024);
        cache.setReplacementPolicy(AppCache.ReplacementPolicy.WTINYLFU);
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = new D1SoftController(adaptive(), cache, new WorkingSetWindow(32), heap::get);
        for (int i = 0; i < 8; i++) {
            c.observe("hot", 0, 1024);
        }
        for (int i = 0; i < 4; i++) {
            c.tick();
        }
        long target = c.targetBudget();
        int block = 64 * 1024;
        for (int i = 0; i < 4; i++) {
            assertThat(cache.put("o", i * (long) block, new byte[block])).isTrue();
            cache.findCovering("o", i * (long) block, (i + 1L) * block);
            cache.findCovering("o", i * (long) block, (i + 1L) * block);
        }
        assertThat(cache.putRequest("o", 8L * block, new byte[3 * block], block)).isFalse();
        assertThat(cache.putRequest("o", 8L * block, new byte[3 * block], block)).isFalse();
        c.tick();
        assertThat(c.targetBudget()).isEqualTo(target);
        assertThat(cache.putRequest("o", 8L * block, new byte[3 * block], block)).isFalse();
        c.tick();

        assertThat(cache.rejectRequestGhostHits()).isGreaterThan(0);
        assertThat(c.targetBudget()).isGreaterThan(target);
        assertThat(c.snapshotFragment()).contains("reject-counterfactual-grow");
    }

    private static RuntimeConfig tinyAdaptive() {
        return RuntimeConfig.builder()
                            .d1Enabled(true)
                            .d1Adaptive(true)
                            .d1AdmitMaxBytes(64)
                            .d1ObserveBudgetBytes(300)
                            .d1HardCacheBytes(4000)
                            .d1MinBudgetBytes(50)
                            .d1TargetCoverage(1.0)
                            .d1ObserveGets(4)
                            .d1HeapHigh(0.70)
                            .d1HeapLow(0.50)
                            .build();
    }

    private static void protectTwoAndRejectThird(AppCache cache) {
        assertThat(cache.put("o", 0, new byte[100])).isTrue();
        assertThat(cache.put("o", 100, new byte[100])).isTrue();
        for (int i = 0; i < 8; i++) {
            cache.findCovering("o", 0, 100);
            cache.findCovering("o", 100, 200);
        }
        assertThat(cache.put("o", 200, new byte[100])).isTrue();
        assertThat(cache.put("o", 300, new byte[100])).isTrue();
        cache.findCovering("o", 300, 400);
        assertThat(cache.put("o", 400, new byte[100])).isFalse();
    }
}
