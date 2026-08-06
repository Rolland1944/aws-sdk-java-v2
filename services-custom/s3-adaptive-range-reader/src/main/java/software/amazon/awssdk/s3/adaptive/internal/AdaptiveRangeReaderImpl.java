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
import software.amazon.awssdk.s3.adaptive.internal.cache.PageCache;
import software.amazon.awssdk.s3.adaptive.internal.exec.LocalityExecutor;
import software.amazon.awssdk.s3.adaptive.internal.exec.MultimodalExecutor;
import software.amazon.awssdk.s3.adaptive.internal.exec.PolicyExecutor;
import software.amazon.awssdk.s3.adaptive.internal.exec.PrefetchExecutor;
import software.amazon.awssdk.s3.adaptive.internal.exec.RandomExecutor;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectChangedException;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectMeta;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.metrics.MetricsRecorder;

/**
 * The feature-flag-on reader: each logical read is turned into an {@link IoRequest} (this is the IO-collection layer),
 * fed to the {@link AdaptivePolicySelector} to pick a policy, and served by that policy's executor over a shared
 * {@link PageCache}. Any executor failure (unknown label, unexpected exception) falls back to an exact demand read so
 * a read never fails for policy reasons; an {@link ObjectChangedException} is surfaced because cached bytes can no
 * longer be trusted.
 */
@SdkInternalApi
public final class AdaptiveRangeReaderImpl extends AbstractRangeReader {

    private final ObjectStore store;
    private final String key;
    private final String versionToken;
    private final String appId;
    private final AdaptivePolicySelector selector;
    private final Map<PolicyName, PolicyExecutor> executors;
    private final MetricsRecorder metrics = new MetricsRecorder();

    public AdaptiveRangeReaderImpl(ObjectStore store, String key, long cacheBudgetBytes, long maxSingleFetchBytes,
                                   String appId) {
        this(store, key, store.head(key), cacheBudgetBytes, maxSingleFetchBytes, appId);
    }

    private AdaptiveRangeReaderImpl(ObjectStore store, String key, ObjectMeta meta, long cacheBudgetBytes,
                                    long maxSingleFetchBytes, String appId) {
        super(meta.contentLength());
        this.store = store;
        this.key = key;
        this.versionToken = meta.versionToken();
        this.appId = appId;
        this.selector = AdaptivePolicySelector.create();

        long size = meta.contentLength();
        String version = meta.versionToken();
        PageCache cache = new PageCache(cacheBudgetBytes);
        this.executors = new EnumMap<>(PolicyName.class);
        executors.put(PolicyName.S3A_RANDOM,
                      new RandomExecutor(store, key, size, version, cache, maxSingleFetchBytes, metrics));
        executors.put(PolicyName.TEMPLATE_LOCALITY,
                      new LocalityExecutor(store, key, size, version, cache, maxSingleFetchBytes, metrics));
        executors.put(PolicyName.TEMPLATE_MULTIMODAL,
                      new MultimodalExecutor(store, key, size, version, cache, maxSingleFetchBytes, metrics));
        executors.put(PolicyName.S3A_PREFETCH,
                      new PrefetchExecutor(store, key, size, version, cache, maxSingleFetchBytes, metrics));
    }

    @Override
    protected int readAt(long position, byte[] dst, int offset, int length) {
        long start = System.nanoTime();
        PolicyName policy = selector.onRead(new IoRequest(key, position, length, (double) objectSize));
        PolicyExecutor executor = executors.get(policy);

        int n;
        boolean fallback = false;
        try {
            if (executor == null) {
                fallback = true;
                n = demandFallback(position, dst, offset, length);
            } else {
                n = executor.serve(position, dst, offset, length);
            }
        } catch (ObjectChangedException e) {
            throw e;
        } catch (RuntimeException e) {
            fallback = true;
            n = demandFallback(position, dst, offset, length);
        }

        metrics.recordRead(policy, n, System.nanoTime() - start, fallback);
        return n;
    }

    private int demandFallback(long position, byte[] dst, int offset, int length) {
        byte[] data = store.getRange(key, position, position + length, versionToken);
        metrics.recordRemoteFetch(data.length);
        System.arraycopy(data, 0, dst, offset, length);
        return length;
    }

    @Override
    public ReaderMetrics metrics() {
        return metrics.snapshot();
    }

    @Override
    public PolicyName currentPolicy() {
        return selector.currentPolicy();
    }

    /**
     * The organization dimension for this reader (see {@code Builder#appId}), or {@code null} if none was set.
     */
    public String appId() {
        return appId;
    }
}
