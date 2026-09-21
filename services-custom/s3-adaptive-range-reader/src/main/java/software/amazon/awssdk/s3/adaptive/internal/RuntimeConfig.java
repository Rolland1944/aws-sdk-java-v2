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

package software.amazon.awssdk.s3.adaptive.internal;

import java.util.Locale;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Immutable, process-level tunables resolved by {@code AdaptiveReaderRuntime.Builder} and shared with every
 * {@code AppContext} and reader it creates.
 */
@SdkInternalApi
public final class RuntimeConfig {

    private final long globalCacheBytes;
    private final long perAppReservedBytes;
    private final int maxConcurrentGets;
    private final long perAppInflightBytes;
    private final int prefetchDepth;
    private final long prefetchBlockSize;
    private final long maxSingleFetchBytes;
    private final boolean d1Enabled;
    private final boolean d2Enabled;
    private final boolean d4Enabled;
    private final long d2WaitWindowMicros;
    private final long d1BlockBytes;
    private final long d1AdmitMaxBytes;
    private final boolean d1Adaptive;
    private final long d1HardCacheBytes;
    private final long d1ObserveBudgetBytes;
    private final long d1MinBudgetBytes;
    private final double d1TargetCoverage;
    private final long d1HorizonBytes;
    private final int d1HorizonEvents;
    private final double d1HeapHigh;
    private final double d1HeapLow;
    private final int d1ObserveGets;
    private final double d4MaxWasteRatio;

    private RuntimeConfig(Builder b) {
        this.globalCacheBytes = b.globalCacheBytes;
        this.perAppReservedBytes = b.perAppReservedBytes;
        this.maxConcurrentGets = b.maxConcurrentGets;
        this.perAppInflightBytes = b.perAppInflightBytes;
        this.prefetchDepth = b.prefetchDepth;
        this.prefetchBlockSize = b.prefetchBlockSize;
        this.maxSingleFetchBytes = b.maxSingleFetchBytes;
        this.d1Enabled = b.d1Enabled;
        this.d2Enabled = b.d2Enabled;
        this.d4Enabled = b.d4Enabled;
        this.d2WaitWindowMicros = Math.max(0L, b.d2WaitWindowMicros);
        this.d1BlockBytes = Math.max(1L, b.d1BlockBytes);
        this.d1AdmitMaxBytes = Math.max(0L, b.d1AdmitMaxBytes);
        this.d1Adaptive = b.d1Adaptive;
        this.d1HardCacheBytes = Math.max(1L, b.d1HardCacheBytes);
        this.d1ObserveBudgetBytes = Math.max(0L, b.d1ObserveBudgetBytes);
        this.d1MinBudgetBytes = Math.max(0L, b.d1MinBudgetBytes);
        this.d1TargetCoverage = clampRatio(b.d1TargetCoverage);
        this.d1HorizonBytes = Math.max(1L, b.d1HorizonBytes);
        this.d1HorizonEvents = Math.max(16, b.d1HorizonEvents);
        this.d1HeapHigh = clampRatio(b.d1HeapHigh);
        this.d1HeapLow = clampRatio(b.d1HeapLow);
        this.d1ObserveGets = Math.max(1, b.d1ObserveGets);
        this.d4MaxWasteRatio = clampRatio(b.d4MaxWasteRatio);
    }

    public static Builder builder() {
        return new Builder();
    }

    public long globalCacheBytes() {
        return globalCacheBytes;
    }

    public long perAppReservedBytes() {
        return perAppReservedBytes;
    }

    public int maxConcurrentGets() {
        return maxConcurrentGets;
    }

    public long perAppInflightBytes() {
        return perAppInflightBytes;
    }

    public int prefetchDepth() {
        return prefetchDepth;
    }

    public long prefetchBlockSize() {
        return prefetchBlockSize;
    }

    public long maxSingleFetchBytes() {
        return maxSingleFetchBytes;
    }

    public boolean d1Enabled() {
        return d1Enabled;
    }

    public boolean d2Enabled() {
        return d2Enabled;
    }

    public boolean d4Enabled() {
        return d4Enabled;
    }

    public long d2WaitWindowMicros() {
        return d2WaitWindowMicros;
    }

    public long d1BlockBytes() {
        return d1BlockBytes;
    }

    /**
     * Ranges larger than this are not admitted to D1. {@code 0} means always
     * admit. Default 256 KiB so scans do not pollute the small-hotspot cache.
     */
    public long d1AdmitMaxBytes() {
        return d1AdmitMaxBytes;
    }

    public boolean d1Admissible(long length) {
        return d1AdmitMaxBytes <= 0L || length <= d1AdmitMaxBytes;
    }

    public boolean d1Adaptive() {
        return d1Adaptive;
    }

    public long d1HardCacheBytes() {
        return d1HardCacheBytes;
    }

    public long d1ObserveBudgetBytes() {
        return d1ObserveBudgetBytes;
    }

    public long d1MinBudgetBytes() {
        return d1MinBudgetBytes;
    }

    public double d1TargetCoverage() {
        return d1TargetCoverage;
    }

    public long d1HorizonBytes() {
        return d1HorizonBytes;
    }

    public int d1HorizonEvents() {
        return d1HorizonEvents;
    }

    public double d1HeapHigh() {
        return d1HeapHigh;
    }

    public double d1HeapLow() {
        return d1HeapLow;
    }

    public int d1ObserveGets() {
        return d1ObserveGets;
    }

    public double d4MaxWasteRatio() {
        return d4MaxWasteRatio;
    }

    /**
     * Spark/S3A process knobs. All dimensions default off so Track 2 E0 is
     * unchanged. D4 without D2 is ignored by the pipeline (merge needs the queue).
     */
    public static RuntimeConfig fromSystemProperties() {
        Builder b = builder();
        b.d1Enabled(boolProp("track1.d1", false));
        b.d2Enabled(boolProp("track1.d2", false));
        b.d4Enabled(boolProp("track1.d4", false));
        b.d2WaitWindowMicros(longProp("track1.d2.wait.us", 0L));
        b.d1BlockBytes(longProp("track1.d1.block.bytes", 1024L * 1024));
        b.d1AdmitMaxBytes(longProp("track1.d1.admit.bytes", 256L * 1024));
        boolean adaptive = boolProp("track1.d1.adaptive", false);
        b.d1Adaptive(adaptive);
        long cacheMiB = longProp("track1.d1.cache.mib", 256L);
        long observeBytes = Math.max(0L, cacheMiB) * 1024L * 1024L;
        b.d1ObserveBudgetBytes(observeBytes);
        b.d1MinBudgetBytes(longProp("track1.d1.min.mib", 16L) * 1024L * 1024L);
        b.d1TargetCoverage(doubleProp("track1.d1.coverage", 1.0));
        b.d1HorizonBytes(longProp("track1.d1.horizon.mib", 4096L) * 1024L * 1024L);
        b.d1HorizonEvents((int) longProp("track1.d1.horizon.events", 131072L));
        b.d1HeapHigh(doubleProp("track1.d1.heap.high", 0.70));
        b.d1HeapLow(doubleProp("track1.d1.heap.low", 0.50));
        b.d1ObserveGets((int) longProp("track1.d1.observe.gets", 256L));
        if (adaptive) {
            long hardMiB = longProp("track1.d1.hard.mib", 4096L);
            long hardBytes = Math.max(observeBytes, hardMiB * 1024L * 1024L);
            b.d1HardCacheBytes(hardBytes);
            b.globalCacheBytes(hardBytes);
            b.perAppReservedBytes(hardBytes);
        } else if (observeBytes > 0) {
            b.d1HardCacheBytes(observeBytes);
            b.globalCacheBytes(observeBytes);
            b.perAppReservedBytes(observeBytes);
        } else {
            b.d1HardCacheBytes(256L * 1024 * 1024);
        }
        b.maxConcurrentGets((int) longProp("track1.d2.workers", 16L));
        long maxFetchMiB = longProp("track1.d4.max.fetch.mib", 8L);
        if (maxFetchMiB > 0) {
            long maxFetch = maxFetchMiB * 1024L * 1024L;
            b.maxSingleFetchBytes(maxFetch);
            b.perAppInflightBytes(Math.max(32L * 1024 * 1024, maxFetch));
        }
        String waste = prop("track1.d4.max.waste");
        if (waste != null) {
            b.d4MaxWasteRatio(Double.parseDouble(waste));
        }
        return b.build();
    }

    private static double clampRatio(double value) {
        if (value < 0.0) {
            return 0.0;
        }
        if (value > 1.0) {
            return 1.0;
        }
        return value;
    }

    private static boolean boolProp(String key, boolean fallback) {
        String raw = prop(key);
        return raw == null ? fallback : Boolean.parseBoolean(raw);
    }

    private static double doubleProp(String key, double fallback) {
        String raw = prop(key);
        if (raw == null) {
            return fallback;
        }
        return Double.parseDouble(raw);
    }

    private static long longProp(String key, long fallback) {
        String raw = prop(key);
        if (raw == null) {
            return fallback;
        }
        return Long.parseLong(raw);
    }

    private static String prop(String key) {
        String value = System.getProperty(key);
        if (value == null || value.isEmpty()) {
            value = System.getenv(key.toUpperCase(Locale.ROOT).replace('.', '_'));
        }
        return value == null || value.isEmpty() ? null : value;
    }

    /**
     * Builder with sane defaults for the tunables.
     */
    @SdkInternalApi
    public static final class Builder {

        private long globalCacheBytes = 256L * 1024 * 1024;
        private long perAppReservedBytes = 32L * 1024 * 1024;
        private int maxConcurrentGets = 16;
        private long perAppInflightBytes = 32L * 1024 * 1024;
        private int prefetchDepth = 1;
        private long prefetchBlockSize = 1024L * 1024;
        private long maxSingleFetchBytes = 8L * 1024 * 1024;
        private boolean d1Enabled;
        private boolean d2Enabled;
        private boolean d4Enabled;
        private long d2WaitWindowMicros;
        private long d1BlockBytes = 1024L * 1024;
        private long d1AdmitMaxBytes = 256L * 1024;
        private boolean d1Adaptive;
        private long d1HardCacheBytes = 256L * 1024 * 1024;
        private long d1ObserveBudgetBytes = 256L * 1024 * 1024;
        private long d1MinBudgetBytes = 16L * 1024 * 1024;
        private double d1TargetCoverage = 1.0;
        private long d1HorizonBytes = 4L * 1024L * 1024L * 1024L;
        private int d1HorizonEvents = 131072;
        private double d1HeapHigh = 0.70;
        private double d1HeapLow = 0.50;
        private int d1ObserveGets = 256;
        private double d4MaxWasteRatio = 0.25;

        private Builder() {
        }

        public Builder globalCacheBytes(long globalCacheBytes) {
            this.globalCacheBytes = globalCacheBytes;
            return this;
        }

        public Builder perAppReservedBytes(long perAppReservedBytes) {
            this.perAppReservedBytes = perAppReservedBytes;
            return this;
        }

        public Builder maxConcurrentGets(int maxConcurrentGets) {
            this.maxConcurrentGets = maxConcurrentGets;
            return this;
        }

        public Builder perAppInflightBytes(long perAppInflightBytes) {
            this.perAppInflightBytes = perAppInflightBytes;
            return this;
        }

        public Builder prefetchDepth(int prefetchDepth) {
            this.prefetchDepth = prefetchDepth;
            return this;
        }

        public Builder prefetchBlockSize(long prefetchBlockSize) {
            this.prefetchBlockSize = prefetchBlockSize;
            return this;
        }

        public Builder maxSingleFetchBytes(long maxSingleFetchBytes) {
            this.maxSingleFetchBytes = maxSingleFetchBytes;
            return this;
        }

        public Builder d1Enabled(boolean d1Enabled) {
            this.d1Enabled = d1Enabled;
            return this;
        }

        public Builder d2Enabled(boolean d2Enabled) {
            this.d2Enabled = d2Enabled;
            return this;
        }

        public Builder d4Enabled(boolean d4Enabled) {
            this.d4Enabled = d4Enabled;
            return this;
        }

        public Builder d2WaitWindowMicros(long d2WaitWindowMicros) {
            this.d2WaitWindowMicros = d2WaitWindowMicros;
            return this;
        }

        public Builder d1BlockBytes(long d1BlockBytes) {
            this.d1BlockBytes = d1BlockBytes;
            return this;
        }

        public Builder d1AdmitMaxBytes(long d1AdmitMaxBytes) {
            this.d1AdmitMaxBytes = d1AdmitMaxBytes;
            return this;
        }

        public Builder d1Adaptive(boolean d1Adaptive) {
            this.d1Adaptive = d1Adaptive;
            return this;
        }

        public Builder d1HardCacheBytes(long d1HardCacheBytes) {
            this.d1HardCacheBytes = d1HardCacheBytes;
            return this;
        }

        public Builder d1ObserveBudgetBytes(long d1ObserveBudgetBytes) {
            this.d1ObserveBudgetBytes = d1ObserveBudgetBytes;
            return this;
        }

        public Builder d1MinBudgetBytes(long d1MinBudgetBytes) {
            this.d1MinBudgetBytes = d1MinBudgetBytes;
            return this;
        }

        public Builder d1TargetCoverage(double d1TargetCoverage) {
            this.d1TargetCoverage = d1TargetCoverage;
            return this;
        }

        public Builder d1HorizonBytes(long d1HorizonBytes) {
            this.d1HorizonBytes = d1HorizonBytes;
            return this;
        }

        public Builder d1HorizonEvents(int d1HorizonEvents) {
            this.d1HorizonEvents = d1HorizonEvents;
            return this;
        }

        public Builder d1HeapHigh(double d1HeapHigh) {
            this.d1HeapHigh = d1HeapHigh;
            return this;
        }

        public Builder d1HeapLow(double d1HeapLow) {
            this.d1HeapLow = d1HeapLow;
            return this;
        }

        public Builder d1ObserveGets(int d1ObserveGets) {
            this.d1ObserveGets = d1ObserveGets;
            return this;
        }

        public Builder d4MaxWasteRatio(double d4MaxWasteRatio) {
            this.d4MaxWasteRatio = d4MaxWasteRatio;
            return this;
        }

        public RuntimeConfig build() {
            return new RuntimeConfig(this);
        }
    }
}
