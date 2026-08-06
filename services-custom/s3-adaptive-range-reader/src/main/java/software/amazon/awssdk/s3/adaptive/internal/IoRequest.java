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

/**
 * One logical range read {@code read(offset, length)} against an object. Mirrors the fields of
 * {@code prefetch_simulator.Request} that the feature extractor consumes.
 *
 * <p>{@code fileSize} is nullable: a {@code null} (or non-positive / NaN) size means "unknown", which the feature
 * extractor treats as {@code cur_size_over_filesize = 0}, matching {@code _valid_file_size} in the Python reference.
 */
@SdkInternalApi
public final class IoRequest {

    private final String objectKey;
    private final long offset;
    private final long length;
    private final Double fileSize;

    public IoRequest(String objectKey, long offset, long length, Double fileSize) {
        this.objectKey = objectKey;
        this.offset = offset;
        this.length = length;
        this.fileSize = fileSize;
    }

    public String objectKey() {
        return objectKey;
    }

    public long offset() {
        return offset;
    }

    public long length() {
        return length;
    }

    /**
     * The end offset (exclusive) of this read: {@code offset + length}.
     */
    public long end() {
        return offset + length;
    }

    /**
     * The object's total size in bytes, or {@code null} if unknown.
     */
    public Double fileSize() {
        return fileSize;
    }
}
