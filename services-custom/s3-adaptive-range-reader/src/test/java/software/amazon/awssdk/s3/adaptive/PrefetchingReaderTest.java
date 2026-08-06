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

package software.amazon.awssdk.s3.adaptive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Random;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.internal.io.InMemoryAsyncObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectChangedException;

/**
 * Functional-equivalence and safety tests for the S3 asynchronous prefetching reader, driven entirely offline through
 * {@link InMemoryAsyncObjectStore}. Verifies byte correctness across access patterns, seek behaviour, object-change
 * surfacing, and that prefetch counters stay consistent.
 */
class PrefetchingReaderTest {

    private static final String BUCKET = "bkt";
    private static final String KEY = "obj.dat";
    private static final long SIZE = 4L * 1024 * 1024;

    private static AdaptiveReaderRuntime runtime(int prefetchDepth) {
        return AdaptiveReaderRuntime.builder()
                                    .globalCacheBytes(64L * 1024 * 1024)
                                    .perAppReservedBytes(16L * 1024 * 1024)
                                    .prefetchDepth(prefetchDepth)
                                    .prefetchBlockSize(64L * 1024)
                                    .maxConcurrentGets(8)
                                    .build();
    }

    private static void assertReadMatches(AdaptiveRangeReader reader, long pos, int len) {
        byte[] dst = new byte[len + 8];
        int n = reader.read(pos, dst, 4, len);
        int wanted = (int) Math.min(len, SIZE - pos);
        assertThat(n).isEqualTo(wanted);
        byte[] got = Arrays.copyOfRange(dst, 4, 4 + n);
        assertThat(got).isEqualTo(InMemoryAsyncObjectStore.expected(pos, pos + n));
    }

    @Test
    void returnsCorrectBytesForMixedPatterns() {
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore().define(KEY, SIZE);
        try (AdaptiveReaderRuntime rt = runtime(1)) {
            AppContext ctx = rt.register("flink");
            AdaptiveRangeReader reader = ctx.newReader(store, BUCKET, KEY);

            for (long p = 0; p < 512 * 1024; p += 8192) {
                assertReadMatches(reader, p, 8192);
            }
            for (int i = 0; i < 5; i++) {
                assertReadMatches(reader, 700_000, 4096);
            }
            assertReadMatches(reader, 100_000, 16384);
            assertReadMatches(reader, 1_500_000, 300_000);
            assertReadMatches(reader, 256 * 1024 - 10, 512);
            Random rnd = new Random(99);
            for (int i = 0; i < 200; i++) {
                long p = rnd.nextInt((int) SIZE - 4096);
                assertReadMatches(reader, p, 1 + rnd.nextInt(4096));
            }

            ReaderMetrics m = reader.metrics();
            assertThat(m.logicalReads()).isGreaterThan(0);
            assertThat(m.remoteBytes()).isGreaterThan(0);
            assertThat(m.readAmplification()).isGreaterThan(0.0).isFinite();
            assertThat(m.cacheHitRate()).isBetween(0.0, 1.0);
            assertThat(m.fallbackReads()).isEqualTo(0);
            // Prefetch attribution stays consistent.
            assertThat(m.prefetchUsefulBytes()).isLessThanOrEqualTo(m.prefetchBytes());
            assertThat(m.prefetchBytes()).isLessThanOrEqualTo(m.remoteBytes());
            assertThat(m.prefetchWastedBytes()).isGreaterThanOrEqualTo(0);
        }
    }

    @Test
    void depthZeroBehavesLikeSynchronousReaderButStillCaches() {
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore().define(KEY, SIZE);
        try (AdaptiveReaderRuntime rt = runtime(0)) {
            AdaptiveRangeReader reader = rt.register("spark").newReader(store, BUCKET, KEY);
            for (long p = 0; p < 256 * 1024; p += 4096) {
                assertReadMatches(reader, p, 4096);
            }
            ReaderMetrics m = reader.metrics();
            assertThat(m.prefetchGets()).isEqualTo(0); // no speculation at depth 0
            assertThat(m.fallbackReads()).isEqualTo(0);
            assertThat(m.cacheHitReads()).isGreaterThan(0); // reads within cached demand blocks still hit
        }
    }

    @Test
    void eofAndStatefulCursorWithSeekCancellation() {
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore().define(KEY, SIZE);
        try (AdaptiveReaderRuntime rt = runtime(2)) {
            AdaptiveRangeReader reader = rt.register("flink").newReader(store, BUCKET, KEY);

            byte[] dst = new byte[1024];
            int n = reader.read(SIZE - 10, dst, 0, 1024);
            assertThat(n).isEqualTo(10);
            assertThat(Arrays.copyOfRange(dst, 0, 10)).isEqualTo(InMemoryAsyncObjectStore.expected(SIZE - 10, SIZE));
            assertThat(reader.read(SIZE, dst, 0, 16)).isEqualTo(-1);

            reader.seek(0);            // must cancel any speculation without breaking subsequent reads
            int a = reader.read(dst, 0, 100);
            reader.seek(2_000_000);    // backward/forward jumps
            int b = reader.read(dst, 0, 100);
            assertThat(a).isEqualTo(100);
            assertThat(b).isEqualTo(100);
            assertThat(reader.position()).isEqualTo(2_000_100);
        }
    }

    @Test
    void objectChangedDuringReadIsSurfaced() {
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore().define(KEY, SIZE);
        try (AdaptiveReaderRuntime rt = runtime(0)) {
            AdaptiveRangeReader reader = rt.register("flink").newReader(store, BUCKET, KEY);

            byte[] dst = new byte[4096];
            reader.read(0, dst, 0, 4096); // pin + cache near start

            store.objectChanged(true);
            assertThatThrownBy(() -> reader.read(3_000_000, dst, 0, 4096))
                .isInstanceOf(ObjectChangedException.class);
        }
    }
}
