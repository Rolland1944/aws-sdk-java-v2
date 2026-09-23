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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.internal.budget.AppBudgetLease;
import software.amazon.awssdk.s3.adaptive.internal.budget.GlobalBudget;
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

class Track1RangeCacheTest {

    @Test
    void coveringHitAndVersionIsolation() {
        Track1RangeCache cache = newCache(1024);
        GetObjectRequest v1 = req("k", "v1");
        GetObjectRequest v2 = req("k", "v2");
        byte[] data = bytes(200);
        cache.put(v1, null, 100, data);

        assertThat(read(cache.tryHit(v1, 120, 180))).isEqualTo(slice(data, 20, 80));
        assertThat(read(cache.tryHit(v1, 100, 300))).isEqualTo(data);
        assertThat(cache.tryHit(v1, 250, 350)).isNull();
        assertThat(cache.tryHit(v2, 120, 180)).isNull();
    }

    @Test
    void stitchesAdjacentOneMegPieces() {
        Track1RangeCache cache = newCache(64);
        GetObjectRequest req = req("k", null);
        cache.put(req, null, 0, bytes(128));
        Track1RangeCache.Hit hit = cache.tryHit(req, 10, 100);
        byte[] actual = read(hit);
        assertThat(actual).hasSize(90);
        assertThat(actual[0]).isEqualTo((byte) 10);
        assertThat(actual[89]).isEqualTo((byte) 99);
    }

    @Test
    void unversionedLookupDoesNotUseResponseEtagAsPrimaryKey() {
        Track1RangeCache cache = newCache(1024);
        GetObjectRequest req = req("k", null);
        cache.put(req, software.amazon.awssdk.services.s3.model.GetObjectResponse.builder()
                                                                               .eTag("\"abc\"")
                                                                               .build(),
                  0, bytes(32));
        assertThat(read(cache.tryHit(req, 0, 32))).hasSize(32);
        assertThat(cache.tryHit(req, 0, 32).eTag).isEqualTo("\"abc\"");
    }

    @Test
    void splitRequestKeepsOriginalSizeBucket() {
        GlobalBudget budget = new GlobalBudget(8L * 1024 * 1024);
        AppBudgetLease lease = new AppBudgetLease(budget, "t");
        AppCache app = new AppCache(lease);
        budget.register("t", 8L * 1024 * 1024, app);
        app.setReplacementPolicy(AppCache.ReplacementPolicy.WTINYLFU);
        Track1RangeCache cache = new Track1RangeCache(app, 1024L * 1024L);
        GetObjectRequest req = req("k", null);
        int requestBytes = 2 * 1024 * 1024;
        cache.put(req, null, 0, bytes(requestBytes));

        assertThat(app.requestObserved(requestBytes)).isEqualTo(1);
        assertThat(app.requestAdmitted(requestBytes)).isEqualTo(1);
        assertThat(app.requestRejected(requestBytes)).isEqualTo(0);
        assertThat(app.requestAdmitted(1024 * 1024)).isEqualTo(0);
        assertThat(app.requestResident(requestBytes)).isEqualTo(requestBytes);
        assertThat(cache.tryHit(req, 0, requestBytes)).isNotNull();
        assertThat(app.requestHits(requestBytes)).isEqualTo(1);
        assertThat(app.requestUseful(requestBytes)).isEqualTo(requestBytes);
    }

    @Test
    void openHitPinsPayloadUntilStreamCloses() throws Exception {
        GlobalBudget budget = new GlobalBudget(4);
        AppBudgetLease lease = new AppBudgetLease(budget, "t");
        AppCache app = new AppCache(lease);
        budget.register("t", 4, app);
        Track1RangeCache cache = new Track1RangeCache(app, 4);
        GetObjectRequest request = req("pinned", null);
        cache.put(request, null, 0, new byte[] {1, 2, 3, 4});

        Track1RangeCache.Hit hit = cache.tryHit(request, 0, 4);
        assertThat(hit.stream.read()).isEqualTo(1);
        assertThat(app.put("other", 0, new byte[] {5, 6, 7, 8})).isFalse();

        hit.stream.close();
        assertThat(app.put("other", 0, new byte[] {5, 6, 7, 8})).isTrue();
    }

    private static Track1RangeCache newCache(long block) {
        GlobalBudget budget = new GlobalBudget(4L * 1024 * 1024);
        AppBudgetLease lease = new AppBudgetLease(budget, "t");
        AppCache app = new AppCache(lease);
        budget.register("t", 4L * 1024 * 1024, app);
        return new Track1RangeCache(app, block);
    }

    private static GetObjectRequest req(String key, String version) {
        return GetObjectRequest.builder().bucket("b").key(key).versionId(version).build();
    }

    private static byte[] bytes(int n) {
        byte[] b = new byte[n];
        for (int i = 0; i < n; i++) {
            b[i] = (byte) i;
        }
        return b;
    }

    private static byte[] slice(byte[] data, int from, int to) {
        byte[] out = new byte[to - from];
        System.arraycopy(data, from, out, 0, out.length);
        return out;
    }

    private static byte[] read(Track1RangeCache.Hit hit) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream(hit.length);
            byte[] buf = new byte[32];
            for (int n; (n = hit.stream.read(buf)) >= 0;) {
                out.write(buf, 0, n);
            }
            hit.stream.close();
            return out.toByteArray();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
