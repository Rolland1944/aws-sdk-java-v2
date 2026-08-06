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

import java.util.EnumMap;
import java.util.Map;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.PolicyName;
import software.amazon.awssdk.s3.adaptive.ReaderMetrics;
import software.amazon.awssdk.s3.adaptive.internal.budget.ConcurrencyLimiter;
import software.amazon.awssdk.s3.adaptive.internal.budget.InflightLimiter;
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache;
import software.amazon.awssdk.s3.adaptive.internal.cache.CachedBlock;
import software.amazon.awssdk.s3.adaptive.internal.exec.FetchPlan;
import software.amazon.awssdk.s3.adaptive.internal.exec.FetchRange;
import software.amazon.awssdk.s3.adaptive.internal.exec.LocalityPlanner;
import software.amazon.awssdk.s3.adaptive.internal.exec.MultimodalPlanner;
import software.amazon.awssdk.s3.adaptive.internal.exec.PolicyPlanner;
import software.amazon.awssdk.s3.adaptive.internal.exec.PrefetchPlanner;
import software.amazon.awssdk.s3.adaptive.internal.exec.RandomPlanner;
import software.amazon.awssdk.s3.adaptive.internal.io.AsyncObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectChangedException;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectMeta;
import software.amazon.awssdk.s3.adaptive.internal.metrics.MetricsRecorder;
import software.amazon.awssdk.s3.adaptive.internal.prefetch.Prefetcher;

/**
 * The S3 asynchronous, prefetching reader (one per stream). Each logical read is turned into an {@link IoRequest} and
 * fed to the app's <b>shared, cross-object</b> {@link SharedPolicySelector}; the chosen policy's per-reader
 * {@link PolicyPlanner} shapes a demand range and (for prefetch/locality) speculative look-ahead. Demand bytes come
 * from the per-app {@link AppCache} on a hit, otherwise from the {@link Prefetcher} (which deduplicates against
 * in-flight speculation); look-ahead is then scheduled asynchronously. Any planner/IO error falls back to an exact
 * demand read so a read never fails for policy reasons; an {@link ObjectChangedException} is surfaced. A {@code seek}
 * cancels no-longer-relevant speculative fetches.
 *
 * <p>With {@code prefetchDepth == 0} no speculation is scheduled, so behaviour matches the synchronous S2 reader
 * (except the selector is per-app cross-object and the cache is per-app multi-object).
 */
@SdkInternalApi
public final class PrefetchingRangeReaderImpl extends AbstractRangeReader {

    private final String objectId;
    private final String featureKey;
    private final long versionSize;
    private final SharedPolicySelector selector;
    private final AppCache cache;
    private final Map<PolicyName, PolicyPlanner> planners;
    private final MetricsRecorder metrics = new MetricsRecorder();
    private final Prefetcher prefetcher;
    private final long maxSingleFetchBytes;
    private final int prefetchDepth;

    public PrefetchingRangeReaderImpl(AsyncObjectStore store, String key, String featureKey, ObjectMeta meta,
                                      AppCache cache, SharedPolicySelector selector, ConcurrencyLimiter concurrency,
                                      InflightLimiter inflight, RuntimeConfig config) {
        super(meta.contentLength());
        this.featureKey = featureKey;
        this.versionSize = meta.contentLength();
        String versionToken = meta.versionToken();
        this.objectId = featureKey + "#" + versionToken;
        this.cache = cache;
        this.selector = selector;
        this.maxSingleFetchBytes = config.maxSingleFetchBytes();
        this.prefetchDepth = config.prefetchDepth();

        this.planners = new EnumMap<>(PolicyName.class);
        planners.put(PolicyName.S3A_RANDOM, new RandomPlanner());
        planners.put(PolicyName.TEMPLATE_MULTIMODAL, new MultimodalPlanner());
        planners.put(PolicyName.TEMPLATE_LOCALITY, new LocalityPlanner(prefetchDepth));
        planners.put(PolicyName.S3A_PREFETCH, new PrefetchPlanner(config.prefetchBlockSize(), prefetchDepth));

        this.prefetcher = new Prefetcher(store, key, objectId, versionToken, versionSize, maxSingleFetchBytes,
                                         cache, metrics, concurrency, inflight);
    }

    @Override
    protected int readAt(long position, byte[] dst, int offset, int length) {
        long startNanos = System.nanoTime();
        PolicyName policy = selector.onRead(new IoRequest(featureKey, position, length, (double) versionSize));
        PolicyPlanner planner = planners.get(policy);

        boolean fallback = false;
        int n;
        try {
            if (planner == null) {
                fallback = true;
                n = demandExact(position, dst, offset, length);
            } else {
                n = serve(planner, position, dst, offset, length);
            }
        } catch (ObjectChangedException e) {
            throw e;
        } catch (RuntimeException e) {
            fallback = true;
            n = demandExact(position, dst, offset, length);
        }

        metrics.recordRead(policy, n, System.nanoTime() - startNanos, fallback);
        return n;
    }

    private int serve(PolicyPlanner planner, long position, byte[] dst, int offset, int length) {
        long reqEnd = position + length;
        FetchPlan plan = planner.plan(position, length);

        CachedBlock hit = cache.findCovering(objectId, position, reqEnd);
        if (hit != null) {
            System.arraycopy(hit.data(), (int) (position - hit.start()), dst, offset, length);
            metrics.recordCacheHitRead(length);
            if (prefetcher.wasPrefetched(hit.start())) {
                metrics.recordPrefetchUseful(length);
            }
        } else {
            FetchRange demand = plan.demand();
            long fetchStart = Math.max(0L, Math.min(demand.start(), position));
            long fetchEnd = Math.min(versionSize, Math.max(demand.endExclusive(), reqEnd));
            if (fetchEnd - fetchStart > maxSingleFetchBytes) {
                metrics.recordDemandClamp();
                fetchStart = position;
                fetchEnd = reqEnd;
            }
            byte[] data = prefetcher.demand(fetchStart, fetchEnd);
            System.arraycopy(data, (int) (position - fetchStart), dst, offset, length);
        }

        if (prefetchDepth > 0 && !plan.prefetch().isEmpty()) {
            prefetcher.schedule(plan.prefetch());
        }
        return length;
    }

    private int demandExact(long position, byte[] dst, int offset, int length) {
        byte[] data = prefetcher.demand(position, position + length);
        System.arraycopy(data, 0, dst, offset, length);
        return length;
    }

    @Override
    protected void onSeek(long position) {
        prefetcher.cancelSpeculative();
    }

    @Override
    public ReaderMetrics metrics() {
        return metrics.snapshot();
    }

    @Override
    public PolicyName currentPolicy() {
        return selector.currentPolicy();
    }

    @Override
    public void close() {
        prefetcher.close();
    }
}
