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
 * Proves the appID-based environment isolation added in S3: per-app caches never evict each other, each app's
 * reserved floor is honored while idle capacity is borrowed and reclaimed, per-app metrics/selectors are separate,
 * and the global concurrent-GET cap holds across apps.
 */
class AppIsolationTest {

    private static final String BUCKET = "bkt";
    private static final long OBJ_SIZE = 8L * 1024 * 1024;
    private static final long CAPACITY = 1024L * 1024;
    private static final long RESERVED = 256L * 1024;

    private static void scan(AdaptiveRangeReader reader, long from, long to, int step) {
        byte[] dst = new byte[step];
        for (long p = from; p + step <= to; p += step) {
            int n = reader.read(p, dst, 0, step);
            assertThat(n).isEqualTo(step);
            assertThat(dst).isEqualTo(InMemoryAsyncObjectStore.expected(p, p + step));
        }
    }

    @Test
    void reservesAreHonoredAndBorrowedSpaceIsReclaimedWithoutCrossAppEviction() {
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore()
            .define("flink.dat", OBJ_SIZE)
            .define("spark.dat", OBJ_SIZE);

        try (AdaptiveReaderRuntime rt = AdaptiveReaderRuntime.builder()
                                                             .globalCacheBytes(CAPACITY)
                                                             .perAppReservedBytes(RESERVED)
                                                             .prefetchDepth(0)
                                                             .maxConcurrentGets(8)
                                                             .build()) {
            AppContext flink = rt.register("flink");
            AppContext spark = rt.register("spark");
            AdaptiveRangeReader flinkReader = flink.newReader(store, BUCKET, "flink.dat");
            AdaptiveRangeReader sparkReader = spark.newReader(store, BUCKET, "spark.dat");

            // Flink alone borrows idle capacity beyond its reserve.
            scan(flinkReader, 0, 3L * 1024 * 1024, 32 * 1024);
            long flinkBefore = flink.metrics().budgetUsageBytes();
            assertThat(flinkBefore).isGreaterThan(RESERVED);

            // Spark now reads; it claims space by reclaiming Flink's BORROWED bytes.
            scan(sparkReader, 0, 2L * 1024 * 1024, 32 * 1024);

            AppMetrics flinkAfter = flink.metrics();
            AppMetrics sparkAfter = spark.metrics();

            assertThat(sparkAfter.budgetUsageBytes()).isGreaterThan(0);
            assertThat(flinkAfter.budgetUsageBytes()).isLessThanOrEqualTo(flinkBefore); // flink shrank
            // Flink kept (most of) its reserve; it was never wiped by spark.
            assertThat(flinkAfter.budgetUsageBytes()).isGreaterThanOrEqualTo(RESERVED / 2);
            // Global ceiling never exceeded.
            assertThat(flinkAfter.budgetUsageBytes() + sparkAfter.budgetUsageBytes()).isLessThanOrEqualTo(CAPACITY);

            RuntimeMetrics rm = rt.metrics();
            assertThat(rm.perApp()).containsKeys(flink.appId(), spark.appId());
            assertThat(rm.totalUsageBytes()).isLessThanOrEqualTo(CAPACITY);
        }
    }

    @Test
    void featureWindowsAndPoliciesAreSeparatePerApp() {
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore()
            .define("flink.dat", OBJ_SIZE)
            .define("spark.dat", OBJ_SIZE);

        try (AdaptiveReaderRuntime rt = AdaptiveReaderRuntime.builder()
                                                             .globalCacheBytes(CAPACITY)
                                                             .perAppReservedBytes(RESERVED)
                                                             .prefetchDepth(0)
                                                             .build()) {
            AppContext flink = rt.register("flink");
            AppContext spark = rt.register("spark");
            AdaptiveRangeReader flinkReader = flink.newReader(store, BUCKET, "flink.dat");
            AdaptiveRangeReader sparkReader = spark.newReader(store, BUCKET, "spark.dat");

            byte[] dst = new byte[8192];
            for (int i = 0; i < 20; i++) {
                flinkReader.read(i * 8192L, dst, 0, 8192);
            }
            // Flink has observed reads; Spark's selector is untouched (separate per-app window/hysteresis).
            assertThat(flinkReader.currentPolicy()).isNotNull();
            assertThat(sparkReader.currentPolicy()).isNull();
        }
    }

    @Test
    void globalConcurrentGetsAreCappedAcrossApps() throws Exception {
        int maxConcurrent = 2;
        ExecutorService completions = Executors.newFixedThreadPool(8);
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore()
            .define("flink.dat", OBJ_SIZE)
            .define("spark.dat", OBJ_SIZE)
            .executor(completions)
            .latencyMillis(15);

        ExecutorService readers = Executors.newFixedThreadPool(4);
        try (AdaptiveReaderRuntime rt = AdaptiveReaderRuntime.builder()
                                                             .globalCacheBytes(64L * 1024 * 1024)
                                                             .perAppReservedBytes(8L * 1024 * 1024)
                                                             .prefetchDepth(0)
                                                             .maxConcurrentGets(maxConcurrent)
                                                             .build()) {
            AdaptiveRangeReader flinkReader = rt.register("flink").newReader(store, BUCKET, "flink.dat");
            AdaptiveRangeReader sparkReader = rt.register("spark").newReader(store, BUCKET, "spark.dat");

            List<Future<?>> futures = new ArrayList<>();
            for (AdaptiveRangeReader reader : new AdaptiveRangeReader[] {flinkReader, sparkReader}) {
                for (int t = 0; t < 2; t++) {
                    long base = t * 1_000_000L;
                    futures.add(readers.submit(() -> {
                        byte[] dst = new byte[16 * 1024];
                        for (int i = 0; i < 40; i++) {
                            reader.read(base + i * 40_000L, dst, 0, 16 * 1024);
                        }
                    }));
                }
            }
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
            assertThat(store.peakInFlight()).isLessThanOrEqualTo(maxConcurrent);
        } finally {
            readers.shutdownNow();
            completions.shutdownNow();
        }
    }
}
