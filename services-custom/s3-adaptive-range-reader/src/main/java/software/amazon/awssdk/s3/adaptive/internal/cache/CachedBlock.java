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

package software.amazon.awssdk.s3.adaptive.internal.cache;

import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * A contiguous cached byte range for one object version: {@code data} holds the bytes for
 * {@code [start, start + data.length)}.
 */
@SdkInternalApi
public final class CachedBlock {

    private final long start;
    private final byte[] data;

    public CachedBlock(long start, byte[] data) {
        this.start = start;
        this.data = data;
    }

    public long start() {
        return start;
    }

    public long end() {
        return start + data.length;
    }

    public int length() {
        return data.length;
    }

    /**
     * Backing bytes. Not copied: callers must only read.
     */
    public byte[] data() {
        return data;
    }
}
