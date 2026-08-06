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
import software.amazon.awssdk.s3.adaptive.internal.AdaptiveRangeReaderImpl;
import software.amazon.awssdk.s3.adaptive.internal.PassthroughRangeReader;
import software.amazon.awssdk.s3.adaptive.internal.io.InMemoryObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectChangedException;

class AdaptiveRangeReaderFunctionalTest {

    private static final String KEY = "obj.dat";
    private static final int SIZE = 3 * 1024 * 1024;
    private static final long CACHE_BUDGET = 64L * 1024 * 1024;
    private static final long MAX_SINGLE_FETCH = 8L * 1024 * 1024;

    private static byte[] canonical() {
        byte[] b = new byte[SIZE];
        for (int i = 0; i < SIZE; i++) {
            b[i] = (byte) (i * 31 + 7);
        }
        return b;
    }

    private static InMemoryObjectStore store(byte[] data) {
        InMemoryObjectStore store = new InMemoryObjectStore();
        store.put(KEY, data);
        return store;
    }

    private static AdaptiveRangeReader adaptive(InMemoryObjectStore store) {
        return new AdaptiveRangeReaderImpl(store, KEY, CACHE_BUDGET, MAX_SINGLE_FETCH, "test-app");
    }

    private static void assertReadMatches(AdaptiveRangeReader reader, byte[] expected, long pos, int len) {
        byte[] dst = new byte[len + 8];
        int n = reader.read(pos, dst, 4, len);
        int wanted = (int) Math.min(len, expected.length - pos);
        assertThat(n).isEqualTo(wanted);
        byte[] got = Arrays.copyOfRange(dst, 4, 4 + n);
        byte[] exp = Arrays.copyOfRange(expected, (int) pos, (int) pos + n);
        assertThat(got).isEqualTo(exp);
    }

    @Test
    void passthroughReturnsExactBytesAndNoAmplification() {
        byte[] data = canonical();
        PassthroughRangeReader reader = new PassthroughRangeReader(store(data), KEY);

        assertThat(reader.size()).isEqualTo(SIZE);
        assertReadMatches(reader, data, 0, 65536);
        assertReadMatches(reader, data, 1_000_000, 4096);
        assertReadMatches(reader, data, 42, 100);

        ReaderMetrics m = reader.metrics();
        assertThat(m.logicalReads()).isEqualTo(3);
        assertThat(m.remoteGets()).isEqualTo(3);
        assertThat(m.readAmplification()).isEqualTo(1.0); // demand-exact, byte-identical
        assertThat(reader.currentPolicy()).isNull();
    }

    @Test
    void adaptiveReturnsCorrectBytesForMixedPatterns() {
        byte[] data = canonical();
        AdaptiveRangeReader reader = adaptive(store(data));

        // sequential
        for (long p = 0; p < 512 * 1024; p += 8192) {
            assertReadMatches(reader, data, p, 8192);
        }
        // repeated / locality (same page revisited)
        for (int i = 0; i < 5; i++) {
            assertReadMatches(reader, data, 700_000, 4096);
        }
        // backward seek
        assertReadMatches(reader, data, 100_000, 16384);
        // large read
        assertReadMatches(reader, data, 1_500_000, 300_000);
        // cross-page small reads
        assertReadMatches(reader, data, 256 * 1024 - 10, 512);
        // random
        Random rnd = new Random(1234);
        for (int i = 0; i < 200; i++) {
            long p = rnd.nextInt(SIZE - 4096);
            assertReadMatches(reader, data, p, 1 + rnd.nextInt(4096));
        }

        ReaderMetrics m = reader.metrics();
        assertThat(m.logicalReads()).isGreaterThan(0);
        assertThat(m.remoteBytes()).isGreaterThan(0);
        assertThat(m.readAmplification()).isGreaterThan(0.0).isFinite();
        assertThat(m.cacheHitRate()).isBetween(0.0, 1.0);
        // repeated reads of the same page + reads within a fetched block must produce cache hits
        assertThat(m.cacheHitReads()).isGreaterThan(0);
        assertThat(m.fallbackReads()).isEqualTo(0);
    }

    @Test
    void eofAndStatefulCursorSemantics() {
        byte[] data = canonical();
        AdaptiveRangeReader reader = adaptive(store(data));

        byte[] dst = new byte[1024];
        // read spanning EOF returns only available bytes
        int n = reader.read(SIZE - 10, dst, 0, 1024);
        assertThat(n).isEqualTo(10);
        assertThat(Arrays.copyOfRange(dst, 0, 10)).isEqualTo(Arrays.copyOfRange(data, SIZE - 10, SIZE));
        // at/after EOF -> -1
        assertThat(reader.read(SIZE, dst, 0, 16)).isEqualTo(-1);

        // stateful cursor advances
        reader.seek(0);
        int a = reader.read(dst, 0, 100);
        int b = reader.read(dst, 0, 100);
        assertThat(a).isEqualTo(100);
        assertThat(b).isEqualTo(100);
        assertThat(reader.position()).isEqualTo(200);
    }

    @Test
    void singleFetchBudgetIsNeverExceeded() {
        byte[] data = canonical();
        InMemoryObjectStore store = store(data);
        long tightBudget = 128L * 1024;
        AdaptiveRangeReader reader =
            new AdaptiveRangeReaderImpl(store, KEY, CACHE_BUDGET, tightBudget, null);

        for (long p = 0; p < SIZE - 8192; p += 5000) {
            byte[] dst = new byte[8192];
            reader.read(p, dst, 0, 8192);
        }
        // every GET is capped to the budget (all reads are smaller than it)
        assertThat(store.maxSingleFetchBytes()).isLessThanOrEqualTo(tightBudget);
    }

    @Test
    void objectChangedDuringReadIsSurfaced() {
        byte[] data = canonical();
        InMemoryObjectStore store = store(data);
        AdaptiveRangeReader reader = adaptive(store);

        byte[] dst = new byte[4096];
        reader.read(0, dst, 0, 4096); // pins version, caches near start

        // mutate the object -> version token changes
        byte[] data2 = canonical();
        data2[0] = (byte) ~data2[0];
        store.put(KEY, data2);

        // a read that misses the cache must refetch and detect the version change
        assertThatThrownBy(() -> reader.read(2_000_000, dst, 0, 4096))
            .isInstanceOf(ObjectChangedException.class);
    }
}
