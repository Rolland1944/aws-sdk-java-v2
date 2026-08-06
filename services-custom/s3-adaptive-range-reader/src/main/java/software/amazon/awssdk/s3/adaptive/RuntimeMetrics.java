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

package software.amazon.awssdk.s3.adaptive;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import software.amazon.awssdk.annotations.SdkPublicApi;

/**
 * An immutable, process-level snapshot of an {@link AdaptiveReaderRuntime}: the global cache capacity and its total
 * usage, the concurrent-GET cap and observed peak, and a per-app breakdown ({@link AppMetrics}). The per-app detail
 * lets callers verify isolation (each app stays within its own footprint, reserves are honored, the global cap is
 * never exceeded).
 */
@SdkPublicApi
public final class RuntimeMetrics {

    private final long capacityBytes;
    private final long totalUsageBytes;
    private final int maxConcurrentGets;
    private final int concurrentGetPeak;
    private final Map<String, AppMetrics> perApp;

    RuntimeMetrics(long capacityBytes, long totalUsageBytes, int maxConcurrentGets, int concurrentGetPeak,
                   Map<String, AppMetrics> perApp) {
        this.capacityBytes = capacityBytes;
        this.totalUsageBytes = totalUsageBytes;
        this.maxConcurrentGets = maxConcurrentGets;
        this.concurrentGetPeak = concurrentGetPeak;
        this.perApp = Collections.unmodifiableMap(new LinkedHashMap<>(perApp));
    }

    public long capacityBytes() {
        return capacityBytes;
    }

    public long totalUsageBytes() {
        return totalUsageBytes;
    }

    public int maxConcurrentGets() {
        return maxConcurrentGets;
    }

    public int concurrentGetPeak() {
        return concurrentGetPeak;
    }

    /**
     * Per-app resource footprints, keyed by appId.
     */
    public Map<String, AppMetrics> perApp() {
        return perApp;
    }

    @Override
    public String toString() {
        return "RuntimeMetrics{capacityBytes=" + capacityBytes
               + ", totalUsageBytes=" + totalUsageBytes
               + ", maxConcurrentGets=" + maxConcurrentGets
               + ", concurrentGetPeak=" + concurrentGetPeak
               + ", perApp=" + perApp
               + '}';
    }
}
