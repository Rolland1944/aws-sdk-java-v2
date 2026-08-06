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

package software.amazon.awssdk.s3.adaptive.internal.io;

import java.util.concurrent.CompletableFuture;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Asynchronous counterpart of {@link ObjectStore}, used by the S3 prefetching reader. Ranged reads return a
 * {@link CompletableFuture} so the reader can overlap demand IO with speculative prefetch, deduplicate in-flight
 * fetches, and cancel no-longer-needed prefetches on a {@code seek}. Isolating {@code S3AsyncClient} behind this SPI
 * keeps the reader/cache/prefetch logic testable offline via {@code InMemoryAsyncObjectStore}.
 *
 * <p>Physical layer only: it knows nothing about features, policies, budget, or the cache.
 */
@SdkInternalApi
public interface AsyncObjectStore {

    /**
     * Fetch object metadata (size + version token). Called once per reader to pin a version and learn the size.
     * Synchronous because it happens once, outside the hot path.
     */
    ObjectMeta head(String key);

    /**
     * Asynchronously read {@code [start, endExclusive)}. The completed future holds exactly {@code endExclusive -
     * start} bytes. The future completes exceptionally with an {@link ObjectChangedException} if the live version no
     * longer matches {@code expectedVersionToken}. Cancelling the returned future should abort the fetch where
     * possible and must release any resources associated with it.
     *
     * @param key                  object key.
     * @param start                inclusive start offset.
     * @param endExclusive         exclusive end offset.
     * @param expectedVersionToken the version pinned by the reader, or {@code null} to skip the check.
     */
    CompletableFuture<byte[]> getRange(String key, long start, long endExclusive, String expectedVersionToken);
}
