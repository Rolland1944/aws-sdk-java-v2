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

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.s3.adaptive.internal.RuntimeConfig;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Sync {@code getObject} arm for S3A. D1/D2/D4 are independently toggleable.
 * Any cache/queue/merge fault returns {@code null} so the proxy falls back to
 * the original exact-range GET. Service errors propagate unchanged.
 *
 * <p>D1 only admits ranges {@code ≤ d1AdmitMaxBytes} (default 256 KiB). Larger
 * misses pass through the original streaming GET so scans do not occupy the
 * cache or get copied into a single {@code byte[]}. Admitted misses on the
 * sync path are teed: the caller reads the live S3 stream, and a complete
 * consume publishes the bytes into the cache.
 */
@SdkInternalApi
public final class Track1GetPipeline {

    private final Track1S3aRuntime runtime;

    public Track1GetPipeline(Track1S3aRuntime runtime) {
        this.runtime = runtime;
    }

    public boolean enabled() {
        return runtime.dimensionsEnabled();
    }

    /**
     * @return a stream for the requested range, or {@code null} to passthrough
     */
    public ResponseInputStream<GetObjectResponse> trySyncGet(S3Client sync,
                                                             GetObjectRequest request) throws Exception {
        if (request == null || !enabled()) {
            return null;
        }
        long[] range = Track1S3aProbe.parseRange(request.range());
        if (range[0] < 0 || range[1] < range[0]) {
            return null;
        }
        long start = range[0];
        long endExclusive = range[1] + 1;
        long length = endExclusive - start;
        RuntimeConfig cfg = runtime.config();
        try {
            if (cfg.d1Enabled()) {
                String objectId = Track1RangeCache.objectId(request, null);
                Track1RangeCache.Hit hit = runtime.cache().tryHit(request, start, endExclusive);
                if (hit != null) {
                    runtime.controller().observe(objectId, start, length);
                    runtime.stats().cacheHit(hit.length);
                    GetObjectResponse meta = hit.eTag == null
                        ? null
                        : GetObjectResponse.builder().eTag(hit.eTag).build();
                    return synthetic(hit.stream, hit.length, meta);
                }
                // Observe after the admit check so "seen before" excludes this miss.
                if (!runtime.controller().admit(objectId, start, length)) {
                    runtime.controller().observe(objectId, start, length);
                    runtime.stats().rejectAdmit();
                    return null;
                }
                runtime.controller().observe(objectId, start, length);
                runtime.stats().cacheMiss();
                if (cfg.d1Adaptive() && cfg.d1Doorkeeper()
                    && !runtime.appCache().seenByDoorkeeper(objectId, start)) {
                    return openTiming(sync, request, length);
                }
            } else if (!cfg.d2Enabled() || runtime.asyncClient() == null) {
                return null;
            }
            return fetch(sync, request, start, range[1], cfg.d1Enabled());
        } catch (Throwable t) {
            Throwable cause = unwrap(t);
            if (isServiceError(cause)) {
                if (cause instanceof Exception) {
                    throw (Exception) cause;
                }
                throw new CompletionException(cause);
            }
            runtime.stats().fallback();
            return null;
        }
    }

    private ResponseInputStream<GetObjectResponse> fetch(S3Client sync, GetObjectRequest request,
                                                         long start, long endInclusive,
                                                         boolean cache) throws Exception {
        RuntimeConfig cfg = runtime.config();
        S3AsyncClient async = runtime.asyncClient();
        if (cfg.d2Enabled() && async != null) {
            Track1GetQueue.Fetched fetched;
            try {
                fetched = runtime.queue()
                                 .submit(async, request, start, endInclusive)
                                 .get();
            } catch (ExecutionException e) {
                throw unwrapAsException(e.getCause());
            }
            if (cache) {
                runtime.cache().put(request, fetched.response, start, fetched.bytes);
            }
            return synthetic(fetched.bytes, fetched.response);
        }
        if (cache) {
            return openTee(sync, request, start, endInclusive - start + 1);
        }
        return null;
    }

    private ResponseInputStream<GetObjectResponse> openTee(S3Client sync, GetObjectRequest request,
                                                           long start, long length) throws IOException {
        if (length <= 0 || length > Integer.MAX_VALUE) {
            return null;
        }
        runtime.stats().remoteGet();
        runtime.stats().teedGet(length);
        long setupStart = System.nanoTime();
        ResponseInputStream<GetObjectResponse> in = sync.getObject(request);
        long setupNanos = System.nanoTime() - setupStart;
        return new ResponseInputStream<GetObjectResponse>(
            in.response(),
            new CachingTeeStream(in, request, in.response(), start, (int) length, setupNanos));
    }

    private ResponseInputStream<GetObjectResponse> openTiming(S3Client sync, GetObjectRequest request,
                                                              long length) throws IOException {
        runtime.stats().remoteGet();
        long setupStart = System.nanoTime();
        ResponseInputStream<GetObjectResponse> in = sync.getObject(request);
        long setupNanos = System.nanoTime() - setupStart;
        return new ResponseInputStream<GetObjectResponse>(
            in.response(), new TimingStream(in, Math.max(0L, length), setupNanos));
    }

    private static ResponseInputStream<GetObjectResponse> synthetic(byte[] data,
                                                                    GetObjectResponse meta) {
        GetObjectResponse.Builder b = meta == null ? GetObjectResponse.builder() : meta.toBuilder();
        GetObjectResponse response = b.contentLength((long) data.length).build();
        return new ResponseInputStream<GetObjectResponse>(response, new ByteArrayInputStream(data));
    }

    private static ResponseInputStream<GetObjectResponse> synthetic(InputStream stream, long length,
                                                                    GetObjectResponse meta) {
        GetObjectResponse.Builder b = meta == null ? GetObjectResponse.builder() : meta.toBuilder();
        GetObjectResponse response = b.contentLength(length).build();
        return new ResponseInputStream<GetObjectResponse>(response, stream);
    }

    private static boolean isServiceError(Throwable t) {
        return t instanceof S3Exception;
    }

    private static Throwable unwrap(Throwable t) {
        Throwable cur = t;
        while ((cur instanceof CompletionException || cur instanceof ExecutionException)
               && cur.getCause() != null) {
            cur = cur.getCause();
        }
        return cur;
    }

    private static Exception unwrapAsException(Throwable t) {
        Throwable cause = unwrap(t);
        if (cause instanceof Exception) {
            return (Exception) cause;
        }
        return new CompletionException(cause);
    }

    private final class CachingTeeStream extends FilterInputStream {
        private final GetObjectRequest request;
        private final GetObjectResponse response;
        private final long start;
        private final byte[] buf;
        private final long setupNanos;
        private long readBlockedNanos;
        private int filled;
        private boolean published;

        CachingTeeStream(InputStream in, GetObjectRequest request, GetObjectResponse response,
                         long start, int length, long setupNanos) {
            super(in);
            this.request = request;
            this.response = response;
            this.start = start;
            this.buf = new byte[length];
            this.setupNanos = Math.max(0L, setupNanos);
        }

        @Override
        public int read() throws IOException {
            long t0 = System.nanoTime();
            int b = in.read();
            readBlockedNanos += Math.max(0L, System.nanoTime() - t0);
            if (b >= 0) {
                acceptByte((byte) b);
            } else {
                publishIfComplete();
            }
            return b;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            long t0 = System.nanoTime();
            int n = in.read(b, off, len);
            readBlockedNanos += Math.max(0L, System.nanoTime() - t0);
            if (n > 0) {
                accept(b, off, n);
            } else {
                publishIfComplete();
            }
            return n;
        }

        @Override
        public void close() throws IOException {
            try {
                publishIfComplete();
            } finally {
                in.close();
            }
        }

        private void accept(byte[] src, int off, int n) {
            int room = buf.length - filled;
            if (room <= 0) {
                return;
            }
            int copy = Math.min(room, n);
            long copyStart = runtime.config().d1Profile() ? System.nanoTime() : 0L;
            System.arraycopy(src, off, buf, filled, copy);
            if (runtime.config().d1Profile()) {
                runtime.stats().teeCopy(copy, System.nanoTime() - copyStart);
            }
            filled += copy;
            if (filled == buf.length) {
                publishIfComplete();
            }
        }

        private void acceptByte(byte value) {
            if (filled >= buf.length) {
                return;
            }
            long copyStart = runtime.config().d1Profile() ? System.nanoTime() : 0L;
            buf[filled++] = value;
            if (runtime.config().d1Profile()) {
                runtime.stats().teeCopy(1L, System.nanoTime() - copyStart);
            }
            if (filled == buf.length) {
                publishIfComplete();
            }
        }

        private void publishIfComplete() {
            if (published || filled != buf.length) {
                return;
            }
            published = true;
            runtime.cache().put(request, response, start, buf);
            long remoteNanos = setupNanos + readBlockedNanos;
            runtime.appCache().recordRemoteCost(buf.length, remoteNanos);
            runtime.link().record(buf.length, remoteNanos);
        }
    }

    private final class TimingStream extends FilterInputStream {
        private final long bytes;
        private final long setupNanos;
        private long readBlockedNanos;
        private boolean recorded;

        TimingStream(InputStream in, long bytes, long setupNanos) {
            super(in);
            this.bytes = bytes;
            this.setupNanos = setupNanos;
        }

        @Override
        public int read() throws IOException {
            long start = System.nanoTime();
            int value = in.read();
            readBlockedNanos += Math.max(0L, System.nanoTime() - start);
            if (value < 0) {
                record();
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            long start = System.nanoTime();
            int read = in.read(bytes, offset, length);
            readBlockedNanos += Math.max(0L, System.nanoTime() - start);
            if (read < 0) {
                record();
            }
            return read;
        }

        @Override
        public void close() throws IOException {
            try {
                record();
            } finally {
                in.close();
            }
        }

        private void record() {
            if (!recorded && bytes > 0L) {
                recorded = true;
                long remoteNanos = setupNanos + readBlockedNanos;
                runtime.appCache().recordRemoteCost(bytes, remoteNanos);
                runtime.link().record(bytes, remoteNanos);
            }
        }
    }
}
