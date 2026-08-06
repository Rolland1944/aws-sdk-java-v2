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
import software.amazon.awssdk.s3.adaptive.internal.cache.PageCache;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.metrics.MetricsRecorder;

/**
 * {@code template_locality}: detect a revisited 256 KiB page and widen the fetch to the whole page (so future reads
 * of the page hit cache); otherwise a small 64 KiB readahead. Delegates shaping to {@link LocalityPlanner}
 * (no speculative prefetch in the synchronous S2 path).
 */
@SdkInternalApi
public final class LocalityExecutor extends AbstractPolicyExecutor {

    private final PolicyPlanner planner = new LocalityPlanner();

    public LocalityExecutor(ObjectStore store, String key, long objectSize, String versionToken, PageCache cache,
                            long maxSingleFetchBytes, MetricsRecorder metrics) {
        super(store, key, objectSize, versionToken, cache, maxSingleFetchBytes, metrics);
    }

    @Override
    protected PolicyPlanner planner() {
        return planner;
    }
}
