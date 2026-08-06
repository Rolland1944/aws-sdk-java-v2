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

import software.amazon.awssdk.annotations.SdkPublicApi;

/**
 * An immutable snapshot of one app's (query engine's) isolated resource footprint within an
 * {@link AdaptiveReaderRuntime}: its reserved budget floor, current shared-budget usage and cache occupancy, and its
 * speculative in-flight prefetch bytes (current + peak). Distinct per-app values are the evidence that isolation
 * holds (no app's cache/budget bleeds into another's).
 */
@SdkPublicApi
public final class AppMetrics {

    private final String appId;
    private final long reservedBytes;
    private final long budgetUsageBytes;
    private final long cachedBytes;
    private final int blockCount;
    private final long inflightCurrentBytes;
    private final long inflightPeakBytes;

    AppMetrics(String appId, long reservedBytes, long budgetUsageBytes, long cachedBytes, int blockCount,
               long inflightCurrentBytes, long inflightPeakBytes) {
        this.appId = appId;
        this.reservedBytes = reservedBytes;
        this.budgetUsageBytes = budgetUsageBytes;
        this.cachedBytes = cachedBytes;
        this.blockCount = blockCount;
        this.inflightCurrentBytes = inflightCurrentBytes;
        this.inflightPeakBytes = inflightPeakBytes;
    }

    public String appId() {
        return appId;
    }

    /**
     * The guaranteed budget floor this app can always cache into.
     */
    public long reservedBytes() {
        return reservedBytes;
    }

    /**
     * Bytes this app currently accounts against the shared global budget.
     */
    public long budgetUsageBytes() {
        return budgetUsageBytes;
    }

    /**
     * Bytes currently held in this app's cache.
     */
    public long cachedBytes() {
        return cachedBytes;
    }

    public int blockCount() {
        return blockCount;
    }

    public long inflightCurrentBytes() {
        return inflightCurrentBytes;
    }

    public long inflightPeakBytes() {
        return inflightPeakBytes;
    }

    @Override
    public String toString() {
        return "AppMetrics{appId=" + appId
               + ", reservedBytes=" + reservedBytes
               + ", budgetUsageBytes=" + budgetUsageBytes
               + ", cachedBytes=" + cachedBytes
               + ", blockCount=" + blockCount
               + ", inflightCurrentBytes=" + inflightCurrentBytes
               + ", inflightPeakBytes=" + inflightPeakBytes
               + '}';
    }
}
