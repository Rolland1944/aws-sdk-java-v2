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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.internal.io.InMemoryAsyncObjectStore;

/**
 * The S3 policy selector is per-app and shared across all of an app's readers/objects (restoring the training-time
 * cross-object feature window). This verifies that (a) reads span multiple objects through one shared, cross-object
 * selector while returning correct bytes, and (b) the shared selector is thread-safe under concurrent readers.
 */
class CrossObjectSelectorTest {

    private static final String BUCKET = "bkt";
    private static final long OBJ_SIZE = 4L * 1024 * 1024;

    private static AdaptiveReaderRuntime runtime() {
        return AdaptiveReaderRuntime.builder()
                                    .globalCacheBytes(64L * 1024 * 1024)
                                    .perAppReservedBytes(32L * 1024 * 1024)
                                    .prefetchDepth(1)
                                    .maxConcurrentGets(8)
                                    .build();
    }

    @Test
    void oneAppReadsManyObjectsThroughSharedSelectorWithCorrectBytes() {
        String[] keys = {"a.dat", "b.dat", "c.dat"};
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore();
        for (String k : keys) {
            store.define(k, OBJ_SIZE);
        }
        try (AdaptiveReaderRuntime rt = runtime()) {
            AppContext app = rt.register("flink");
            List<AdaptiveRangeReader> readers = new ArrayList<>();
            for (String k : keys) {
                readers.add(app.newReader(store, BUCKET, k));
            }
            // Interleave reads across objects; all served by the one shared per-app selector.
            for (int round = 0; round < 32; round++) {
                for (AdaptiveRangeReader reader : readers) {
                    long p = (long) round * 32 * 1024;
                    byte[] dst = new byte[16 * 1024];
                    int n = reader.read(p, dst, 0, 16 * 1024);
                    assertThat(n).isEqualTo(16 * 1024);
                    assertThat(dst).isEqualTo(InMemoryAsyncObjectStore.expected(p, p + 16 * 1024));
                }
            }
            assertThat(app.metrics().budgetUsageBytes()).isGreaterThan(0);
        }
    }

    @Test
    void sharedSelectorIsThreadSafeUnderConcurrentReaders() throws Exception {
        int objects = 4;
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore();
        for (int i = 0; i < objects; i++) {
            store.define("o" + i + ".dat", OBJ_SIZE);
        }
        ExecutorService pool = Executors.newFixedThreadPool(objects);
        try (AdaptiveReaderRuntime rt = runtime()) {
            AppContext app = rt.register("flink");
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < objects; i++) {
                String key = "o" + i + ".dat";
                AdaptiveRangeReader reader = app.newReader(store, BUCKET, key);
                futures.add(pool.submit(() -> {
                    byte[] dst = new byte[8192];
                    for (int r = 0; r < 200; r++) {
                        long p = (long) r * 8192;
                        int n = reader.read(p, dst, 0, 8192);
                        assertThat(n).isEqualTo(8192);
                        assertThat(dst).isEqualTo(InMemoryAsyncObjectStore.expected(p, p + 8192));
                    }
                }));
            }
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS); // must not deadlock or throw
            }
            assertThat(app.metrics().budgetUsageBytes()).isGreaterThan(0);
        } finally {
            pool.shutdownNow();
        }
    }
}
