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

/**
 * Synthetic traces that stand in for the P3-0 SF1 / SF8 fixed points. Same
 * controller knobs on every vector: policy-driven admission with a 256 KiB
 * cold-start reference, coverage 1.0, 4 GiB horizon.
 */
class D1DualWindowGoldenTest {

    private static final long KIB = 1024L;
    private static final long MIB = 1024L * 1024L;
    private static final long GIB = 1024L * MIB;
    private static final long ADMIT = 256L * KIB;
    private static final long HORIZON = 4L * GIB;

    private static AppCache cache(long capacity) {
        GlobalBudget budget = new GlobalBudget(capacity);
        AppBudgetLease lease = new AppBudgetLease(budget, "app");
        AppCache cache = new AppCache(lease);
        budget.register("app", capacity, cache);
        return cache;
    }

    private static RuntimeConfig online() {
        return RuntimeConfig.builder()
                            .d1Enabled(true)
                            .d1Adaptive(true)
                            .d1AdmitMaxBytes(ADMIT)
                            .d1ObserveBudgetBytes(256L * MIB)
                            .d1HardCacheBytes(4L * GIB)
                            .d1MinBudgetBytes(16L * MIB)
                            .d1TargetCoverage(1.0)
                            .d1HorizonBytes(HORIZON)
                            .d1HorizonEvents(131072)
                            .d1ObserveGets(8)
                            .d1HeapHigh(0.70)
                            .d1HeapLow(0.50)
                            .build();
    }

    private static D1SoftController controller(AtomicReference<Double> heap) {
        return new D1SoftController(
            online(),
            cache(4L * GIB),
            new WorkingSetWindow(HORIZON, 131072, ADMIT),
            new WorkingSetWindow(HORIZON, 131072, ADMIT),
            heap::get);
    }

    private static void settle(D1SoftController c) {
        for (int i = 0; i < 8; i++) {
            c.tick();
        }
    }

    @Test
    void sf1SmallWorkingSetStaysAtOrBelow256MiB() {
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = controller(heap);
        long range = 128L * KIB;
        int objects = 80;
        for (int i = 0; i < objects; i++) {
            c.observe("sf1-" + i, 0, range);
            c.observe("sf1-" + i, 0, range);
        }
        settle(c);
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.TRACK);
        assertThat(c.budgetWindow().reusableBytes()).isEqualTo(objects * range);
        assertThat(c.targetBudget()).isEqualTo(256L * MIB);
        assertThat(c.admitMax()).isEqualTo(8L * MIB);
        assertThat(c.admit("sf1-0", 0, range)).isTrue();
        assertThat(c.admit("scan", 0, 4L * MIB)).isTrue();
    }

    @Test
    void sf8TelemetryDoesNotGrowCapacityWithoutEvictionGhostEvidence() {
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = controller(heap);
        long range = ADMIT;
        int objects = 4000;
        for (int pass = 0; pass < 2; pass++) {
            for (int i = 0; i < objects; i++) {
                c.observe("sf8-" + i, 0, range);
            }
        }
        settle(c);
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.TRACK);
        assertThat(c.budgetWindow().reusableBytes()).isEqualTo(objects * range);
        assertThat(c.targetBudget()).isEqualTo(256L * MIB);
        assertThat(c.admitMax()).isEqualTo(8L * MIB);
        assertThat(c.admit("sf8-0", 0, range)).isTrue();
        assertThat(c.admit("scan", 0, 4L * MIB)).isTrue();
    }

    @Test
    void pureScanStaysTrackAndLeavesRejectionToPolicyLayer() {
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = controller(heap);
        for (int i = 0; i < 64; i++) {
            c.observe("scan-" + i, 0, 4L * MIB);
        }
        settle(c);
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.TRACK);
        assertThat(c.targetBudget()).isEqualTo(256L * MIB);
        assertThat(c.budgetWindow().reusableBytes()).isEqualTo(0);
        assertThat(c.admit("scan-0", 0, 4L * MIB)).isTrue();
        assertThat(c.admit("tiny", 0, 1024)).isTrue();
    }

    @Test
    void mixKeepsHotspotTelemetryAndLeavesScanDecisionToPolicy() {
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = controller(heap);
        for (int i = 0; i < 40; i++) {
            c.observe("scan-" + i, 0, 4L * MIB);
        }
        for (int pass = 0; pass < 3; pass++) {
            for (int i = 0; i < 30; i++) {
                c.observe("hot-" + i, 0, 128L * KIB);
            }
        }
        settle(c);
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.TRACK);
        assertThat(c.budgetWindow().reusableBytes()).isEqualTo(30L * 128L * KIB);
        assertThat(c.admissionWindow().pollution()).isGreaterThan(0.5);
        assertThat(c.admitMax()).isEqualTo(8L * MIB);
        assertThat(c.admit("hot-0", 0, 128L * KIB)).isTrue();
        assertThat(c.admit("scan-0", 0, 4L * MIB)).isTrue();
    }

    @Test
    void heapHighWaterWinsOverEstimator() {
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = controller(heap);
        for (int pass = 0; pass < 2; pass++) {
            for (int i = 0; i < 80; i++) {
                c.observe("hot-" + i, 0, 128L * KIB);
            }
        }
        settle(c);
        long before = c.targetBudget();
        assertThat(before).isGreaterThan(0);
        heap.set(0.90);
        c.tick();
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.SHRINK);
        assertThat(c.targetBudget()).isLessThanOrEqualTo(Math.max(16L * MIB, before / 2));
        assertThat(c.shrinks()).isGreaterThan(0);
    }

    @Test
    void scanDoesNotPreventLaterHotspotAdmission() {
        AtomicReference<Double> heap = new AtomicReference<Double>(0.2);
        D1SoftController c = controller(heap);
        for (int i = 0; i < 16; i++) {
            c.observe("scan-" + i, 0, 4L * MIB);
        }
        settle(c);
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.TRACK);
        for (int pass = 0; pass < 3; pass++) {
            for (int i = 0; i < 20; i++) {
                c.observe("late-hot-" + i, 0, 128L * KIB);
            }
        }
        settle(c);
        assertThat(c.mode()).isEqualTo(D1SoftController.Mode.TRACK);
        assertThat(c.admitMax()).isEqualTo(8L * MIB);
        assertThat(c.targetBudget()).isGreaterThan(0);
        assertThat(c.admit("late-hot-0", 0, 128L * KIB)).isTrue();
    }
}
