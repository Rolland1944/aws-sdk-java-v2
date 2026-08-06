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

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.s3.adaptive.internal.PrefetchingRangeReaderImpl;
import software.amazon.awssdk.s3.adaptive.internal.RuntimeConfig;
import software.amazon.awssdk.s3.adaptive.internal.SharedPolicySelector;
import software.amazon.awssdk.s3.adaptive.internal.budget.AppBudgetLease;
import software.amazon.awssdk.s3.adaptive.internal.budget.ConcurrencyLimiter;
import software.amazon.awssdk.s3.adaptive.internal.budget.InflightLimiter;
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache;
import software.amazon.awssdk.s3.adaptive.internal.io.AsyncObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectMeta;
import software.amazon.awssdk.s3.adaptive.internal.io.S3AsyncClientObjectStore;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * A per-app (per query engine, e.g. one Flink or Spark deployment) handle into an {@link AdaptiveReaderRuntime}. It
 * owns the app's <b>isolated</b> environment: a private multi-object {@link AppCache}, a lease on the shared
 * work-conserving budget, a per-app in-flight prefetch cap, and a per-app cross-object policy selector. Every reader
 * created here is bound to this environment, so an app's cache/budget/metrics never bleed into another's. The shared
 * concurrency limiter and the immutable decision-tree model are the only process-wide resources it references.
 */
@SdkPublicApi
public final class AppContext {

    private final String appId;
    private final RuntimeConfig config;
    private final AppBudgetLease lease;
    private final AppCache cache;
    private final ConcurrencyLimiter concurrency;
    private final InflightLimiter inflight;
    private final SharedPolicySelector selector;
    private final S3AsyncClient s3Async;

    @SdkInternalApi
    AppContext(String appId, RuntimeConfig config, AppBudgetLease lease, AppCache cache,
               ConcurrencyLimiter concurrency, InflightLimiter inflight, SharedPolicySelector selector,
               S3AsyncClient s3Async) {
        this.appId = appId;
        this.config = config;
        this.lease = lease;
        this.cache = cache;
        this.concurrency = concurrency;
        this.inflight = inflight;
        this.selector = selector;
        this.s3Async = s3Async;
    }

    public String appId() {
        return appId;
    }

    /**
     * Open a reader over {@code bucket/key} using the runtime's {@link S3AsyncClient}.
     */
    public AdaptiveRangeReader newReader(String bucket, String key) {
        return newReader(bucket, key, null);
    }

    /**
     * Open a reader over a specific object version using the runtime's {@link S3AsyncClient}.
     */
    public AdaptiveRangeReader newReader(String bucket, String key, String versionId) {
        if (s3Async == null) {
            throw new IllegalStateException("runtime has no S3AsyncClient; use newReader(AsyncObjectStore, ...)");
        }
        return newReader(new S3AsyncClientObjectStore(s3Async, bucket, versionId), bucket, key);
    }

    /**
     * Open a reader backed by an explicit {@link AsyncObjectStore} (offline tests / custom backends). {@code bucket}
     * and {@code key} only form the logical object identity used for cache keys and cross-object features.
     */
    @SdkInternalApi
    public AdaptiveRangeReader newReader(AsyncObjectStore store, String bucket, String key) {
        String featureKey = bucket + "/" + key;
        ObjectMeta meta = store.head(key);
        return new PrefetchingRangeReaderImpl(store, key, featureKey, meta, cache, selector, concurrency, inflight,
                                              config);
    }

    /**
     * A snapshot of this app's isolated resource footprint.
     */
    public AppMetrics metrics() {
        return new AppMetrics(appId, lease.reserved(), lease.usage(), cache.cachedBytes(), cache.blockCount(),
                              inflight.current(), inflight.peak());
    }
}
