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

import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.internal.RuntimeConfig;
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache;

/**
 * Soft cache controller: the {@link AppCache} object always exists. This only
 * changes admission and the soft target. Bypass stops new puts and trims
 * toward empty; it does not rebuild or clear-stop-the-world.
 *
 * <p>Admission and budget are independent. The admission window sees every
 * range; the budget window sees only ranges that currently pass
 * {@code admit_max}. Spare budget never raises the cap. When
 * {@link RuntimeConfig#d1Adaptive()} is false the controller still observes
 * both windows for telemetry, but admission stays on the frozen
 * {@code d1AdmitMaxBytes} and the soft target stays at the configured budget.
 */
@SdkInternalApi
public final class D1SoftController {

    public enum Mode {
        OBSERVE,
        TRACK,
        BYPASS,
        SHRINK
    }

    interface HeapProbe {
        double usedRatio();
    }

    private static final long KIB = 1024L;
    private static final long FALLBACK_ADMIT = 256L * KIB;
    private static final long FALLBACK_BUDGET = 256L * 1024L * 1024L;

    private final RuntimeConfig config;
    private final AppCache cache;
    private final WorkingSetWindow admission;
    private final WorkingSetWindow budget;
    private final HeapProbe heap;
    private final long conservativeAdmit;
    private final AtomicLong observations = new AtomicLong();
    private final AtomicLong shrinks = new AtomicLong();
    private final AtomicLong bypassDecisions = new AtomicLong();
    private final AtomicInteger hold = new AtomicInteger();
    private volatile Mode mode = Mode.OBSERVE;
    private volatile long targetBudget;
    private volatile long admitMax;
    private volatile double lastHeapRatio = -1.0;

    public D1SoftController(RuntimeConfig config, AppCache cache) {
        this(config, cache,
             newWindow(config),
             newWindow(config),
             JvmHeap.INSTANCE);
    }

    D1SoftController(RuntimeConfig config, AppCache cache, WorkingSetWindow template, HeapProbe heap) {
        this(config, cache, copyWindow(template), copyWindow(template), heap);
    }

    D1SoftController(RuntimeConfig config,
                     AppCache cache,
                     WorkingSetWindow admission,
                     WorkingSetWindow budget,
                     HeapProbe heap) {
        this.config = config;
        this.cache = cache;
        this.admission = admission;
        this.budget = budget;
        this.heap = heap;
        this.conservativeAdmit = conservativeAdmitOf(config);
        this.targetBudget = config.d1ObserveBudgetBytes();
        this.admitMax = this.conservativeAdmit;
        if (config.d1Adaptive()) {
            cache.setSoftCapacity(this.targetBudget);
        }
    }

    public void observe(String objectId, long start, long length) {
        admission.observe(objectId, start, length);
        // Budget telemetry follows the conservative filter, not the live
        // admitMax. Bypass sets admitMax=0 but must still see later reuse
        // or the controller can never leave BYPASS.
        if (length > 0L && length <= conservativeAdmit) {
            budget.observe(objectId, start, length);
        }
        long n = observations.incrementAndGet();
        if (!config.d1Adaptive()) {
            return;
        }
        if (n % 32 == 0 || n == config.d1ObserveGets()) {
            try {
                tick();
            } catch (Throwable ignored) {
                fallbackConservative();
            }
        }
    }

    public boolean admit(String objectId, long start, long length) {
        if (length <= 0) {
            return false;
        }
        if (!config.d1Adaptive()) {
            return config.d1Admissible(length);
        }
        if (mode == Mode.BYPASS || targetBudget <= 0L) {
            return false;
        }
        if (length > admitMax) {
            return false;
        }
        if (mode == Mode.TRACK && budget.reusableBytes() > targetBudget
            && !budget.seenBefore(objectId, start, length)) {
            return false;
        }
        return true;
    }

    public synchronized void tick() {
        double ratio = heap.usedRatio();
        lastHeapRatio = ratio;
        if (ratio >= 0 && ratio >= config.d1HeapHigh()) {
            enter(Mode.SHRINK);
            setTarget(Math.max(config.d1MinBudgetBytes(), targetBudget / 2));
            shrinks.incrementAndGet();
            return;
        }
        if (hold.get() > 0) {
            hold.decrementAndGet();
            return;
        }
        if (observations.get() < config.d1ObserveGets()) {
            enter(Mode.OBSERVE);
            return;
        }
        if (ratio >= 0 && ratio > config.d1HeapLow() && mode == Mode.SHRINK) {
            return;
        }
        long reusable = budget.reusableBytes();
        if (reusable <= 0) {
            enter(Mode.BYPASS);
            setTarget(0);
            bypassDecisions.incrementAndGet();
            return;
        }
        enter(Mode.TRACK);
        long desired = (long) (config.d1TargetCoverage() * reusable);
        desired = clamp(config.d1MinBudgetBytes(), desired, config.d1HardCacheBytes());
        // Grow toward coverage × R_admit. Do not shrink on a lagging
        // estimate: P3-0 SF1 already fits in the 256 MiB observe budget,
        // and shrinking there evicted hits that fixed `100` kept. Heap
        // and BYPASS remain the only downward paths.
        if (desired > targetBudget) {
            setTarget(rateLimit(targetBudget, desired));
        }
        // Admission is a filter, not a function of spare budget. First cut
        // stays on the conservative 256 KiB start; p90 of all-range reuse
        // must not lift the cap when scans happen to repeat.
        admitMax = conservativeAdmit;
    }

    public Mode mode() {
        return mode;
    }

    public long targetBudget() {
        return targetBudget;
    }

    public long admitMax() {
        return admitMax;
    }

    public WorkingSetWindow window() {
        return budget;
    }

    public WorkingSetWindow admissionWindow() {
        return admission;
    }

    public WorkingSetWindow budgetWindow() {
        return budget;
    }

    public long observations() {
        return observations.get();
    }

    public long shrinks() {
        return shrinks.get();
    }

    public long bypassDecisions() {
        return bypassDecisions.get();
    }

    public double lastHeapRatio() {
        return lastHeapRatio;
    }

    public String snapshotFragment() {
        return "\"d1_adaptive\":" + config.d1Adaptive()
            + ",\"d1_mode\":\"" + mode.name().toLowerCase(Locale.ROOT) + "\""
            + ",\"d1_target_budget\":" + targetBudget
            + ",\"d1_admit_max_live\":" + admitMax
            + ",\"d1_u_h\":" + budget.distinctBytes()
            + ",\"d1_r_h\":" + budget.reusableBytes()
            + ",\"d1_r_admit\":" + budget.reusableBytes()
            + ",\"d1_u_all\":" + admission.distinctBytes()
            + ",\"d1_r_all\":" + admission.reusableBytes()
            + ",\"d1_rd_p90\":" + budget.reusedSizePercentile(0.90)
            + ",\"d1_pollution\":" + admission.pollution()
            + ",\"d1_horizon_bytes\":" + admission.horizonBytes()
            + ",\"d1_event_bytes\":" + admission.eventBytes()
            + ",\"d1_heap_ratio\":" + lastHeapRatio
            + ",\"d1_observations\":" + observations.get()
            + ",\"d1_shrinks\":" + shrinks.get()
            + ",\"d1_bypasses\":" + bypassDecisions.get();
    }

    private void enter(Mode next) {
        if (mode != next) {
            mode = next;
            hold.set(2);
        }
    }

    private void setTarget(long next) {
        targetBudget = Math.max(0L, Math.min(next, config.d1HardCacheBytes()));
        cache.setSoftCapacity(targetBudget);
        if (targetBudget == 0L) {
            admitMax = 0L;
        } else if (admitMax <= 0L) {
            admitMax = conservativeAdmit;
        }
    }

    private void fallbackConservative() {
        enter(Mode.OBSERVE);
        admitMax = conservativeAdmit;
        setTarget(Math.min(FALLBACK_BUDGET, config.d1HardCacheBytes()));
    }

    private static WorkingSetWindow newWindow(RuntimeConfig config) {
        return new WorkingSetWindow(config.d1HorizonBytes(),
                                    config.d1HorizonEvents(),
                                    conservativeAdmitOf(config));
    }

    private static WorkingSetWindow copyWindow(WorkingSetWindow src) {
        return new WorkingSetWindow(src.horizonBytes(), src.maxEvents(), src.oversizeMark());
    }

    private static long conservativeAdmitOf(RuntimeConfig config) {
        long configured = config.d1AdmitMaxBytes();
        return configured <= 0L ? FALLBACK_ADMIT : configured;
    }

    private static long rateLimit(long current, long desired) {
        if (current <= 0) {
            return desired;
        }
        if (desired > current * 2) {
            return current * 2;
        }
        if (desired < current / 2) {
            return Math.max(0L, current / 2);
        }
        return desired;
    }

    private static long clamp(long lo, long value, long hi) {
        return Math.max(lo, Math.min(value, hi));
    }

    private static final class JvmHeap implements HeapProbe {
        private static final JvmHeap INSTANCE = new JvmHeap();

        @Override
        public double usedRatio() {
            try {
                Runtime rt = Runtime.getRuntime();
                long max = rt.maxMemory();
                if (max <= 0) {
                    return -1.0;
                }
                long used = rt.totalMemory() - rt.freeMemory();
                return (double) used / (double) max;
            } catch (Throwable ignored) {
                return -1.0;
            }
        }
    }
}
