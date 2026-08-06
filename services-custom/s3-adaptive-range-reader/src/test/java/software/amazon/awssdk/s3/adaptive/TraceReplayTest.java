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
import software.amazon.awssdk.s3.adaptive.internal.AdaptiveRangeReaderImpl;
import software.amazon.awssdk.s3.adaptive.internal.io.GeneratedObjectStore;

/**
 * Replays a real TPC-H trace slice through the adaptive reader and asserts that every read returns the exact expected
 * bytes (independent of which policy was chosen), and that aggregate IO metrics are sane. Policy-quality comparison is
 * out of scope here (deferred to S4); this guards correctness of the collect -> select -> execute path on real access
 * patterns.
 */
class TraceReplayTest {

    private static final long CACHE_BUDGET = 64L * 1024 * 1024;
    private static final long MAX_SINGLE_FETCH = 8L * 1024 * 1024;

    private static final class Rec {
        final String key;
        final long offset;
        final int length;
        final long fileSize;

        Rec(String key, long offset, int length, long fileSize) {
            this.key = key;
            this.offset = offset;
            this.length = length;
            this.fileSize = fileSize;
        }
    }

    private static List<Rec> loadTrace() throws Exception {
        List<Rec> recs = new ArrayList<>();
        try (InputStream in = TraceReplayTest.class.getResourceAsStream("/traces/tpch_slice.csv");
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

    @Test
    void replayReturnsExactBytesAndSaneMetrics() throws Exception {
        List<Rec> recs = loadTrace();
        assertThat(recs).isNotEmpty();

        GeneratedObjectStore store = new GeneratedObjectStore();
        for (Rec r : recs) {
            store.define(r.key, Math.max(r.fileSize, r.offset + r.length));
        }

        Map<String, AdaptiveRangeReader> readers = new HashMap<>();
        long totalLogicalReads = 0;
        long totalLogicalBytes = 0;

        for (Rec r : recs) {
            AdaptiveRangeReader reader = readers.computeIfAbsent(
                r.key, k -> new AdaptiveRangeReaderImpl(store, k, CACHE_BUDGET, MAX_SINGLE_FETCH, null));

            byte[] dst = new byte[r.length];
            int n = reader.read(r.offset, dst, 0, r.length);
            assertThat(n).isGreaterThan(0);
            for (int i = 0; i < n; i++) {
                assertThat(dst[i]).isEqualTo(GeneratedObjectStore.byteAt(r.offset + i));
            }
        }

        for (AdaptiveRangeReader reader : readers.values()) {
            ReaderMetrics m = reader.metrics();
            totalLogicalReads += m.logicalReads();
            totalLogicalBytes += m.logicalBytes();
            // Read amplification may be below 1.0 (caching helps) or above (readahead over-fetch); only require it
            // to be positive and finite, and that no read fell back for policy reasons.
            assertThat(m.remoteBytes()).isGreaterThan(0);
            assertThat(m.readAmplification()).isGreaterThan(0.0).isFinite();
            assertThat(m.cacheHitRate()).isBetween(0.0, 1.0);
            assertThat(m.fallbackReads()).isEqualTo(0);
        }

        assertThat(totalLogicalReads).isEqualTo(recs.size());
        assertThat(totalLogicalBytes).isGreaterThan(0);
        assertThat(store.getCount()).isGreaterThan(0);
        assertThat(store.totalGetBytes()).isGreaterThan(0);
    }
}
