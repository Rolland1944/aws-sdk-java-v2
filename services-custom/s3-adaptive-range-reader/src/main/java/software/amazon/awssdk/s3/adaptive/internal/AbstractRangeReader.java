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

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.AdaptiveRangeReader;

/**
 * Shared stream-cursor mechanics (seek / position / stateful read) plus argument validation and EOF handling for the
 * concrete readers. Subclasses implement the actual per-read fetch via {@link #readAt(long, byte[], int, int)}.
 */
@SdkInternalApi
public abstract class AbstractRangeReader implements AdaptiveRangeReader {

    protected final long objectSize;

    private long cursor;

    protected AbstractRangeReader(long objectSize) {
        this.objectSize = objectSize;
    }

    /**
     * Fetch exactly {@code length} bytes at {@code position} (already validated: {@code length > 0} and
     * {@code position + length <= objectSize}).
     */
    protected abstract int readAt(long position, byte[] dst, int offset, int length);

    @Override
    public final long size() {
        return objectSize;
    }

    @Override
    public final int read(long position, byte[] dst, int offset, int length) {
        if (dst == null) {
            throw new NullPointerException("dst");
        }
        if (position < 0) {
            throw new IllegalArgumentException("position must be >= 0: " + position);
        }
        if (offset < 0 || length < 0 || offset + length > dst.length) {
            throw new IndexOutOfBoundsException("offset=" + offset + " length=" + length + " dst.length=" + dst.length);
        }
        if (position >= objectSize) {
            return -1;
        }
        if (length == 0) {
            return 0;
        }
        int toRead = (int) Math.min(length, objectSize - position);
        return readAt(position, dst, offset, toRead);
    }

    @Override
    public final int read(byte[] dst, int offset, int length) {
        int n = read(cursor, dst, offset, length);
        if (n > 0) {
            cursor += n;
        }
        return n;
    }

    @Override
    public final void seek(long position) {
        if (position < 0) {
            throw new IllegalArgumentException("position must be >= 0: " + position);
        }
        this.cursor = position;
        onSeek(position);
    }

    /**
     * Hook invoked after the cursor moves via {@link #seek(long)}. Default no-op; the prefetching reader overrides it
     * to cancel no-longer-relevant speculative fetches.
     */
    protected void onSeek(long position) {
        // No-op by default.
    }

    @Override
    public final long position() {
        return cursor;
    }

    @Override
    public void close() {
        // No client-owned resources: the ObjectStore/S3Client lifecycle is owned by the caller.
    }
}
