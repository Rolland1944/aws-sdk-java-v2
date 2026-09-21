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
    void noReuseAfterObserveWindowGoesBypass() {
        AppCache cache = cache(4 * 1024 * 1024);
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = new D1SoftController(adaptive(), cache, new WorkingSetWindow(32), heap::get);
        for (int i = 0; i < 8; i++) {
            c.observe("o", i * 1000L, 100);
        }
        c.tick();
        c.tick();
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.BYPASS);
        assertThat(c.targetBudget()).isEqualTo(0);
        assertThat(c.admit("o", 0, 100)).isFalse();
        assertThat(cache.softCapacity()).isEqualTo(0);
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
    void scansDoNotInflateBudgetReusableOrAdmitMax() {
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
        assertThat(c.admitMax()).isEqualTo(256 * 1024);
        assertThat(c.admit("scan-0", 0, 4 * 1024 * 1024)).isFalse();
        assertThat(c.admit("hot", 0, 1024)).isTrue();
    }

    @Test
    void spareBudgetDoesNotRaiseAdmission() {
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
        assertThat(c.admitMax()).isEqualTo(256 * 1024);
        assertThat(c.admit("mid", 0, 300 * 1024)).isFalse();
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
        assertThat(c.admitMax()).isEqualTo(256 * 1024);
    }
}
