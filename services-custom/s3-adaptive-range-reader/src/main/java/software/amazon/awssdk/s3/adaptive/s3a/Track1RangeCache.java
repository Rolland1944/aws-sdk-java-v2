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

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.util.List;
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
    private final Track1S3aStats stats;
    private final boolean profile;
    private final boolean zeroCopy;
    private final boolean sharedBacking;
    private final ConcurrentHashMap<String, String> etags = new ConcurrentHashMap<String, String>();

    public Track1RangeCache(AppCache cache, long blockBytes) {
        this(cache, blockBytes, null, false, true, true);
    }

    Track1RangeCache(AppCache cache, long blockBytes, Track1S3aStats stats, boolean profile, boolean zeroCopy,
                     boolean sharedBacking) {
        this.cache = cache;
        this.blockBytes = Math.max(1L, blockBytes);
        this.stats = stats;
        this.profile = profile;
        this.zeroCopy = zeroCopy;
        this.sharedBacking = sharedBacking;
    }

    public static final class Hit {
        public final InputStream stream;
        public final int length;
        public final String eTag;

        Hit(InputStream stream, int length, String eTag) {
            this.stream = stream;
            this.length = length;
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
        long lookupStart = profile ? System.nanoTime() : 0L;
        AppCache.PinnedRange pinned = cache.pinCoveringSpan(id, start, endExclusive);
        recordLookup(lookupStart);
        if (pinned == null) {
            return null;
        }
        List<CachedBlock> blocks = pinned.blocks();
        if (profile && stats != null && blocks.size() > 1) {
            stats.cacheStitch(System.nanoTime() - lookupStart);
        }
        noteHit(id, blocks.get(0).start(), length);
        if (!zeroCopy) {
            long copyStart = profile ? System.nanoTime() : 0L;
            byte[] copy = materialize(pinned, length);
            if (profile && stats != null) {
                stats.cacheHitCopy(length, System.nanoTime() - copyStart);
            }
            pinned.close();
            return new Hit(new ByteArrayInputStream(copy), length, etags.get(id));
        }
        return new Hit(new PinnedSliceInputStream(pinned), length, etags.get(id));
    }

    public void put(GetObjectRequest request, GetObjectResponse response, long start, byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }
        String id = objectId(request, null);
        long putStart = profile ? System.nanoTime() : 0L;
        boolean admitted = sharedBacking ? cache.putRequestOwned(id, start, data, blockBytes)
                                         : cache.putRequest(id, start, data, blockBytes);
        recordPut(putStart);
        cache.noteOriginalRequest(data.length, admitted);
        if (response != null && response.eTag() != null && !response.eTag().isEmpty()) {
            etags.put(id, response.eTag());
        }
        String requestToken = token(request, null);
        String responseToken = token(null, response);
        if (!"*".equals(responseToken) && !responseToken.equals(requestToken)) {
            String bucket = request == null ? "" : nullToEmpty(request.bucket());
            String key = request == null ? "" : nullToEmpty(request.key());
            putStart = profile ? System.nanoTime() : 0L;
            if (sharedBacking) {
                cache.putRequestOwned(bucket + '\0' + key + '\0' + responseToken, start, data, blockBytes);
            } else {
                cache.putRequest(bucket + '\0' + key + '\0' + responseToken, start, data, blockBytes);
            }
            recordPut(putStart);
        }
    }

    private void noteHit(String id, long blockStart, long useful) {
        long origin = cache.originRequestBytes(id, blockStart);
        if (origin <= 0L) {
            origin = useful;
        }
        cache.noteOriginHit(origin, useful);
    }

    private void recordLookup(long start) {
        if (profile && stats != null) {
            stats.cacheLookup(System.nanoTime() - start);
        }
    }

    private void recordHitCopy(long bytes, long start) {
        if (profile && stats != null) {
            stats.cacheHitCopy(bytes, System.nanoTime() - start);
        }
    }

    private void recordPut(long start) {
        if (profile && stats != null) {
            stats.cachePut(System.nanoTime() - start);
        }
    }

    private static byte[] materialize(AppCache.PinnedRange pinned, int length) {
        byte[] out = new byte[length];
        long position = pinned.start();
        int offset = 0;
        for (CachedBlock block : pinned.blocks()) {
            int count = (int) Math.min(block.end() - position, pinned.end() - position);
            System.arraycopy(block.data(), block.dataOffset() + (int) (position - block.start()), out, offset, count);
            position += count;
            offset += count;
        }
        return out;
    }

    public long cachedBytes() {
        return cache.cachedBytes();
    }

    private static final class PinnedSliceInputStream extends InputStream {
        private final AppCache.PinnedRange pinned;
        private final List<CachedBlock> blocks;
        private long position;
        private int index;
        private boolean closed;

        private PinnedSliceInputStream(AppCache.PinnedRange pinned) {
            this.pinned = pinned;
            this.blocks = pinned.blocks();
            this.position = pinned.start();
        }

        @Override
        public int read() throws IOException {
            if (closed) {
                throw new IOException("stream closed");
            }
            if (position >= pinned.end()) {
                return -1;
            }
            CachedBlock block = blocks.get(index);
            int value = block.data()[block.dataOffset() + (int) (position - block.start())] & 0xff;
            position++;
            if (position >= block.end() && index + 1 < blocks.size()) {
                index++;
            }
            return value;
        }

        @Override
        public int read(byte[] dst, int offset, int length) throws IOException {
            if (closed) {
                throw new IOException("stream closed");
            }
            if (length == 0) {
                return 0;
            }
            if (position >= pinned.end()) {
                return -1;
            }
            int copied = 0;
            while (copied < length && position < pinned.end()) {
                CachedBlock block = blocks.get(index);
                int available = (int) Math.min(block.end() - position, pinned.end() - position);
                int copy = Math.min(available, length - copied);
                System.arraycopy(block.data(), block.dataOffset() + (int) (position - block.start()),
                                 dst, offset + copied, copy);
                copied += copy;
                position += copy;
                if (position >= block.end() && index + 1 < blocks.size()) {
                    index++;
                }
            }
            return copied;
        }

        @Override
        public void close() {
            if (!closed) {
                closed = true;
                pinned.close();
            }
        }
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
