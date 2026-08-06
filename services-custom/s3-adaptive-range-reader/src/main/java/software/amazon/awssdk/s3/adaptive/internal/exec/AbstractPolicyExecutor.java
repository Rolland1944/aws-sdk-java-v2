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

package software.amazon.awssdk.s3.adaptive.internal.exec;

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.PolicyName;
import software.amazon.awssdk.s3.adaptive.internal.cache.CachedBlock;
import software.amazon.awssdk.s3.adaptive.internal.cache.PageCache;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.metrics.MetricsRecorder;

/**
 * Shared cache/fetch/copy pipeline for the synchronous (S2) policy executors. Subclasses only supply the policy
 * brain via {@link #planner()}; this base guarantees correctness (the returned bytes always come from either a single
 * covering cache block or one freshly fetched buffer) and enforces the single-fetch byte budget. Speculative
 * look-ahead from the planner is ignored here (it is realised by the asynchronous S3 reader).
 */
@SdkInternalApi
public abstract class AbstractPolicyExecutor implements PolicyExecutor {

    protected final long objectSize;

    private final ObjectStore store;
    private final String key;
    private final String versionToken;
    private final PageCache cache;
    private final long maxSingleFetchBytes;
    private final MetricsRecorder metrics;

    protected AbstractPolicyExecutor(ObjectStore store, String key, long objectSize, String versionToken,
                                     PageCache cache, long maxSingleFetchBytes, MetricsRecorder metrics) {
        this.store = store;
        this.key = key;
        this.objectSize = objectSize;
        this.versionToken = versionToken;
        this.cache = cache;
        this.maxSingleFetchBytes = maxSingleFetchBytes;
        this.metrics = metrics;
    }

    /**
     * The (stateful, per-reader) policy brain that shapes demand ranges. One instance per executor.
     */
    protected abstract PolicyPlanner planner();

    @Override
    public final PolicyName policy() {
        return planner().policy();
    }

    @Override
    public final int serve(long position, byte[] dst, int dstOffset, int length) {
        long reqEnd = position + length;

        CachedBlock hit = cache.findCovering(position, reqEnd);
        if (hit != null) {
            System.arraycopy(hit.data(), (int) (position - hit.start()), dst, dstOffset, length);
            metrics.recordCacheHitRead(length);
            return length;
        }

        FetchRange planned = planner().plan(position, length).demand();
        long fetchStart = Math.max(0L, Math.min(planned.start(), position));
        long fetchEnd = Math.min(objectSize, Math.max(planned.endExclusive(), reqEnd));
        // Budget guard: never let a policy's over-read exceed the single-fetch cap; fall back to exact demand.
        if (fetchEnd - fetchStart > maxSingleFetchBytes) {
            metrics.recordDemandClamp();
            fetchStart = position;
            fetchEnd = reqEnd;
        }

        byte[] data = store.getRange(key, fetchStart, fetchEnd, versionToken);
        metrics.recordRemoteFetch(data.length);
        cache.put(new CachedBlock(fetchStart, data));

        System.arraycopy(data, (int) (position - fetchStart), dst, dstOffset, length);
        return length;
    }
}
