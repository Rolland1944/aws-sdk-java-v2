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
 * Immutable metadata for an object addressed through an {@link ObjectStore}: its total size and an opaque version
 * token (an S3 eTag or versionId) used to detect mid-read mutation. See PROJECT2 §7.2 (version isolation).
 */
@SdkInternalApi
public final class ObjectMeta {

    private final long contentLength;
    private final String versionToken;

    public ObjectMeta(long contentLength, String versionToken) {
        this.contentLength = contentLength;
        this.versionToken = versionToken;
    }

    /**
     * Total object size in bytes.
     */
    public long contentLength() {
        return contentLength;
    }

    /**
     * Opaque version token (eTag/versionId), or {@code null} if the store does not expose one.
     */
    public String versionToken() {
        return versionToken;
    }
}
