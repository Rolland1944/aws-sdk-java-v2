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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.internal.io.InMemoryAsyncObjectStore;

/**
 * Replays the TPC-H trace slice concurrently through two apps (flink, spark) sharing one runtime, asserting every read
 * returns exact bytes, the two apps stay isolated (separate per-app usage), and the global cache ceiling is respected.
 */
class MultiAppTraceReplayTest {

    private static final String BUCKET = "bkt";
    private static final long CAPACITY = 128L * 1024 * 1024;

    private static final class Rec {
        private final String key;
        private final long offset;
        private final int length;
        private final long fileSize;

        private Rec(String key, long offset, int length, long fileSize) {
            this.key = key;
            this.offset = offset;
            this.length = length;
            this.fileSize = fileSize;
        }
    }

    private static List<Rec> loadTrace() throws Exception {
        List<Rec> recs = new ArrayList<>();
        try (InputStream in = MultiAppTraceReplayTest.class.getResourceAsStream("/traces/tpch_slice.csv");
             BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line = reader.readLine(); // header
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] f = line.split(",");
                recs.add(new Rec(f[1], Long.parseLong(f[2]), Integer.parseInt(f[3]), Long.parseLong(f[4])));
            }
        }
        return recs;
    }

    private static void replay(AppContext app, InMemoryAsyncObjectStore store, List<Rec> recs) {
        Map<String, AdaptiveRangeReader> readers = new HashMap<>();
        for (Rec r : recs) {
            AdaptiveRangeReader reader = readers.computeIfAbsent(r.key, k -> app.newReader(store, BUCKET, k));
            byte[] dst = new byte[r.length];
            int n = reader.read(r.offset, dst, 0, r.length);
            assertThat(n).isGreaterThan(0);
            for (int i = 0; i < n; i++) {
                assertThat(dst[i]).isEqualTo(InMemoryAsyncObjectStore.byteAt(r.offset + i));
            }
            assertThat(reader.metrics().fallbackReads()).isEqualTo(0);
        }
    }

    @Test
    void twoAppsReplayTraceCorrectlyAndStayIsolated() throws Exception {
        List<Rec> recs = loadTrace();
        assertThat(recs).isNotEmpty();

        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore();
        for (Rec r : recs) {
            store.define(r.key, Math.max(r.fileSize, r.offset + r.length));
        }

        try (AdaptiveReaderRuntime rt = AdaptiveReaderRuntime.builder()
                                                             .globalCacheBytes(CAPACITY)
                                                             .perAppReservedBytes(16L * 1024 * 1024)
                                                             .prefetchDepth(1)
                                                             .maxConcurrentGets(8)
                                                             .build()) {
            AppContext flink = rt.register("flink");
            AppContext spark = rt.register("spark");

            replay(flink, store, recs);
            replay(spark, store, recs);

            RuntimeMetrics rm = rt.metrics();
            assertThat(rm.perApp()).containsKeys(flink.appId(), spark.appId());
            assertThat(rm.perApp().get(flink.appId()).budgetUsageBytes()).isGreaterThan(0);
            assertThat(rm.perApp().get(spark.appId()).budgetUsageBytes()).isGreaterThan(0);
            assertThat(rm.totalUsageBytes()).isLessThanOrEqualTo(CAPACITY);
            assertThat(store.getCount()).isGreaterThan(0);
        }
    }
}
