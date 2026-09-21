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

package software.amazon.awssdk.s3.adaptive.s3a;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache;
import software.amazon.awssdk.s3.adaptive.internal.cache.CachedBlock;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * D1: completed ranges land in {@link AppCache} as 1 MiB (configurable) pieces.
 * A later GET hits when cached bytes fully cover it, including adjacent pieces
 * of the same object version. Key is bucket + key + VersionId / If-Match / ETag.
 */
@SdkInternalApi
public final class Track1RangeCache {

    private final AppCache cache;
    private final long blockBytes;
    private final ConcurrentHashMap<String, String> etags = new ConcurrentHashMap<String, String>();

    public Track1RangeCache(AppCache cache, long blockBytes) {
        this.cache = cache;
        this.blockBytes = Math.max(1L, blockBytes);
    }

    public static final class Hit {
        public final byte[] bytes;
        public final String eTag;

        Hit(byte[] bytes, String eTag) {
            this.bytes = bytes;
            this.eTag = eTag;
        }
    }

    public static String objectId(GetObjectRequest request, GetObjectResponse response) {
        String bucket = request == null ? "" : nullToEmpty(request.bucket());
        String key = request == null ? "" : nullToEmpty(request.key());
        return bucket + '\0' + key + '\0' + token(request, response);
    }

    public Hit tryHit(GetObjectRequest request, long start, long endExclusive) {
        if (endExclusive <= start || endExclusive - start > Integer.MAX_VALUE) {
            return null;
        }
        String id = objectId(request, null);
        int length = (int) (endExclusive - start);
        CachedBlock one = cache.findCovering(id, start, endExclusive);
        byte[] bytes = null;
        if (one != null) {
            int from = (int) (start - one.start());
            bytes = Arrays.copyOfRange(one.data(), from, from + length);
        } else {
            byte[] out = new byte[length];
            long pos = start;
            while (pos < endExclusive) {
                CachedBlock block = cache.findCovering(id, pos, pos + 1);
                if (block == null || block.end() <= pos) {
                    return null;
                }
                int copy = (int) (Math.min(endExclusive, block.end()) - pos);
                System.arraycopy(block.data(), (int) (pos - block.start()), out, (int) (pos - start), copy);
                pos += copy;
            }
            bytes = out;
        }
        return new Hit(bytes, etags.get(id));
    }

    public void put(GetObjectRequest request, GetObjectResponse response, long start, byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }
        String id = objectId(request, null);
        putPieces(id, start, data);
        if (response != null && response.eTag() != null && !response.eTag().isEmpty()) {
            etags.put(id, response.eTag());
        }
        String requestToken = token(request, null);
        String responseToken = token(null, response);
        if (!"*".equals(responseToken) && !responseToken.equals(requestToken)) {
            String bucket = request == null ? "" : nullToEmpty(request.bucket());
            String key = request == null ? "" : nullToEmpty(request.key());
            putPieces(bucket + '\0' + key + '\0' + responseToken, start, data);
        }
    }

    private void putPieces(String id, long start, byte[] data) {
        int offset = 0;
        while (offset < data.length) {
            long abs = start + offset;
            long nextBoundary = ((abs / blockBytes) + 1L) * blockBytes;
            int chunk = (int) Math.min(data.length - offset, nextBoundary - abs);
            byte[] piece = Arrays.copyOfRange(data, offset, offset + chunk);
            cache.put(id, abs, piece);
            offset += chunk;
        }
    }

    public long cachedBytes() {
        return cache.cachedBytes();
    }

    static String token(GetObjectRequest request, GetObjectResponse response) {
        if (request != null && request.versionId() != null && !request.versionId().isEmpty()) {
            return "v:" + request.versionId();
        }
        if (request != null && request.ifMatch() != null && !request.ifMatch().isEmpty()) {
            return "e:" + request.ifMatch();
        }
        if (response != null && response.eTag() != null && !response.eTag().isEmpty()) {
            return "e:" + response.eTag();
        }
        return "*";
    }

    private static String nullToEmpty(String value) {
        return value == null ? "" : value;
    }
}
