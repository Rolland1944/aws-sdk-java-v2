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

package software.amazon.awssdk.s3.adaptive.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class PageCacheTest {

    private static byte[] bytes(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) {
            b[i] = (byte) i;
        }
        return b;
    }

    @Test
    void coveringHitAndMiss() {
        PageCache cache = new PageCache(1024);
        cache.put(new CachedBlock(100, bytes(200))); // [100, 300)

        assertThat(cache.findCovering(120, 180)).isNotNull();
        assertThat(cache.findCovering(100, 300)).isNotNull();
        // partial overlap is a miss (no cross-block stitching)
        assertThat(cache.findCovering(250, 350)).isNull();
        assertThat(cache.findCovering(0, 50)).isNull();
    }

    @Test
    void budgetEvictsLeastRecentlyUsed() {
        PageCache cache = new PageCache(300);
        cache.put(new CachedBlock(0, bytes(100)));   // A [0,100)
        cache.put(new CachedBlock(100, bytes(100))); // B [100,200)
        cache.put(new CachedBlock(200, bytes(100))); // C [200,300)  -> full

        // touch A so it is most-recently-used
        assertThat(cache.findCovering(0, 100)).isNotNull();

        cache.put(new CachedBlock(300, bytes(100))); // D -> must evict LRU (B)
        assertThat(cache.cachedBytes()).isEqualTo(300);
        assertThat(cache.findCovering(100, 200)).isNull();  // B evicted
        assertThat(cache.findCovering(0, 100)).isNotNull(); // A survived
        assertThat(cache.findCovering(300, 400)).isNotNull();
    }

    @Test
    void blockLargerThanBudgetIsNotCached() {
        PageCache cache = new PageCache(100);
        cache.put(new CachedBlock(0, bytes(200)));
        assertThat(cache.cachedBytes()).isEqualTo(0);
        assertThat(cache.blockCount()).isEqualTo(0);
    }

    @Test
    void reinsertingSameStartReplacesBytes() {
        PageCache cache = new PageCache(1024);
        cache.put(new CachedBlock(0, bytes(50)));
        cache.put(new CachedBlock(0, bytes(80)));
        assertThat(cache.cachedBytes()).isEqualTo(80);
        assertThat(cache.blockCount()).isEqualTo(1);
        assertThat(cache.findCovering(0, 80)).isNotNull();
    }
}
