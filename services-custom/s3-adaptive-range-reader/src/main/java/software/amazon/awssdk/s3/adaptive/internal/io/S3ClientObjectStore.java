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
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.Validate;

/**
 * The only class that touches the SDK {@code s3} client. Implements {@link ObjectStore} with ranged {@code getObject}
 * calls (buffered fully into memory via {@link ResponseTransformer#toBytes()}) and pins a version via the object eTag.
 *
 * <p>The reader pins the eTag returned by {@link #head(String)}; subsequent {@link #getRange} calls send an
 * {@code If-Match} so that a mid-read overwrite fails with HTTP 412 rather than returning inconsistent bytes.
 */
@SdkInternalApi
public final class S3ClientObjectStore implements ObjectStore {

    private static final int HTTP_PRECONDITION_FAILED = 412;

    private final S3Client s3;
    private final String bucket;
    private final String versionId;

    public S3ClientObjectStore(S3Client s3, String bucket) {
        this(s3, bucket, null);
    }

    public S3ClientObjectStore(S3Client s3, String bucket, String versionId) {
        this.s3 = Validate.paramNotNull(s3, "s3");
        this.bucket = Validate.paramNotBlank(bucket, "bucket");
        this.versionId = versionId;
    }

    @Override
    public ObjectMeta head(String key) {
        HeadObjectResponse resp = s3.headObject(HeadObjectRequest.builder()
                                                                 .bucket(bucket)
                                                                 .key(key)
                                                                 .versionId(versionId)
                                                                 .build());
        Long length = resp.contentLength();
        return new ObjectMeta(length == null ? 0L : length, resp.eTag());
    }

    @Override
    public byte[] getRange(String key, long start, long endExclusive, String expectedVersionToken) {
        if (endExclusive <= start) {
            return new byte[0];
        }
        // HTTP Range is inclusive on both ends.
        String range = "bytes=" + start + "-" + (endExclusive - 1);
        GetObjectRequest request = GetObjectRequest.builder()
                                                   .bucket(bucket)
                                                   .key(key)
                                                   .versionId(versionId)
                                                   .ifMatch(expectedVersionToken)
                                                   .range(range)
                                                   .build();
        try {
            ResponseBytes<GetObjectResponse> resp = s3.getObject(request, ResponseTransformer.toBytes());
            return resp.asByteArrayUnsafe();
        } catch (S3Exception e) {
            if (e.statusCode() == HTTP_PRECONDITION_FAILED) {
                throw new ObjectChangedException("object " + key + " changed during read (If-Match failed): "
                                                 + expectedVersionToken);
            }
            throw e;
        }
    }
}
