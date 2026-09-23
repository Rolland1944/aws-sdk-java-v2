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
 * A contiguous immutable slice for one object version. A slice may share a
 * backing array with adjacent pieces of the same original request.
 */
@SdkInternalApi
public final class CachedBlock {

    private final long start;
    private final byte[] data;
    private final int offset;
    private final int length;

    public CachedBlock(long start, byte[] data) {
        this(start, data, 0, data == null ? 0 : data.length);
    }

    public CachedBlock(long start, byte[] data, int offset, int length) {
        if (data == null || offset < 0 || length < 0 || offset + length > data.length) {
            throw new IllegalArgumentException("invalid cached block slice");
        }
        this.start = start;
        this.data = data;
        this.offset = offset;
        this.length = length;
    }

    public long start() {
        return start;
    }

    public long end() {
        return start + length;
    }

    public int length() {
        return length;
    }

    /**
     * Backing bytes. Not copied: callers must only read.
     */
    public byte[] data() {
        return data;
    }

    /**
     * Offset of this logical block in {@link #data()}.
     */
    public int dataOffset() {
        return offset;
    }
}
