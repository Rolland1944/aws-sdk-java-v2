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

import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Minimal SPI abstracting the physical object backend (S3, or an in-memory test double). Isolating the SDK
 * {@code s3} dependency behind this interface keeps the reader/cache/executor logic testable offline
 * ({@code InMemoryObjectStore}) while {@code S3ClientObjectStore} is the single class that touches the S3 client.
 *
 * <p>This is the physical-fetch layer only: it knows nothing about features, policies, or the page cache. IO feature
 * collection happens one layer up, in the reader's {@code read()} entry point.
 */
@SdkInternalApi
public interface ObjectStore {

    /**
     * Fetch object metadata (size + version token). Used once per reader to pin a version and learn the size.
     */
    ObjectMeta head(String key);

    /**
     * Read the byte range {@code [start, endExclusive)}. Implementations must return exactly {@code endExclusive
     * - start} bytes (the caller only requests ranges within the pinned object size).
     *
     * @param key                  object key.
     * @param start                inclusive start offset.
     * @param endExclusive         exclusive end offset.
     * @param expectedVersionToken the version pinned by the reader, or {@code null} to skip the check.
     * @throws ObjectChangedException if the live version no longer matches {@code expectedVersionToken}.
     */
    byte[] getRange(String key, long start, long endExclusive, String expectedVersionToken);
}
