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

package software.amazon.awssdk.s3.adaptive.internal.prefetch;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.ReaderMetrics;
import software.amazon.awssdk.s3.adaptive.internal.budget.AppBudgetLease;
import software.amazon.awssdk.s3.adaptive.internal.budget.ConcurrencyLimiter;
import software.amazon.awssdk.s3.adaptive.internal.budget.GlobalBudget;
import software.amazon.awssdk.s3.adaptive.internal.budget.InflightLimiter;
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache;
import software.amazon.awssdk.s3.adaptive.internal.exec.FetchRange;
import software.amazon.awssdk.s3.adaptive.internal.io.InMemoryAsyncObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.metrics.MetricsRecorder;

class PrefetcherTest {

    private static final String KEY = "obj.dat";
    private static final String OBJECT_ID = "bucket/obj.dat#v1";
    private static final long SIZE = 8L * 1024 * 1024;
    private static final long MAX_SINGLE_FETCH = 4L * 1024 * 1024;

    private MetricsRecorder metrics;
    private ConcurrencyLimiter concurrency;
    private InflightLimiter inflight;
    private AppCache cache;

    private Prefetcher newPrefetcher(InMemoryAsyncObjectStore store) {
        GlobalBudget budget = new GlobalBudget(64L * 1024 * 1024);
        AppBudgetLease lease = new AppBudgetLease(budget, "app");
        cache = new AppCache(lease);
        budget.register("app", 64L * 1024 * 1024, cache);
        metrics = new MetricsRecorder();
        concurrency = new ConcurrencyLimiter(8);
        inflight = new InflightLimiter(16L * 1024 * 1024);
        return new Prefetcher(store, KEY, OBJECT_ID, "v1", SIZE, MAX_SINGLE_FETCH, cache, metrics, concurrency,
                              inflight);
    }

    @Test
    void demandReturnsExactBytesAndCaches() {
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore().define(KEY, SIZE);
        Prefetcher pf = newPrefetcher(store);

        byte[] got = pf.demand(1000, 1000 + 4096);
        assertThat(got).isEqualTo(InMemoryAsyncObjectStore.expected(1000, 1000 + 4096));
        assertThat(store.getCount()).isEqualTo(1);
        assertThat(cache.findCovering(OBJECT_ID, 1000, 1000 + 4096)).isNotNull();
    }

    @Test
    void demandReusesInFlightPrefetchInsteadOfIssuingAnotherGet() throws Exception {
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore().define(KEY, SIZE).manual(true);
        Prefetcher pf = newPrefetcher(store);

        pf.schedule(Collections.singletonList(new FetchRange(0, 256 * 1024)));
        assertThat(store.getCount()).isEqualTo(1);
        assertThat(store.pendingCount()).isEqualTo(1);

        ExecutorService exec = Executors.newSingleThreadExecutor();
        try {
            CompletableFuture<byte[]> demanded = CompletableFuture.supplyAsync(() -> pf.demand(1024, 1024 + 8192), exec);
            // Give the demand a moment to attach to the in-flight prefetch, then complete it.
            Thread.sleep(100);
            store.completeAll();
            byte[] got = demanded.get(5, TimeUnit.SECONDS);
            assertThat(got).isEqualTo(InMemoryAsyncObjectStore.expected(1024, 1024 + 8192));
        } finally {
            exec.shutdownNow();
        }
        // Only the single speculative GET was issued; the demand reused it.
        assertThat(store.getCount()).isEqualTo(1);
        assertThat(metrics.snapshot().prefetchUsefulBytes()).isEqualTo(8192);
    }

    @Test
    void cancelSpeculativeReleasesBudgetAndCountsCancelledBytes() {
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore().define(KEY, SIZE).manual(true);
        Prefetcher pf = newPrefetcher(store);

        pf.schedule(Collections.singletonList(new FetchRange(0, 256 * 1024)));
        assertThat(inflight.current()).isEqualTo(256 * 1024);

        pf.cancelSpeculative();

        ReaderMetrics m = metrics.snapshot();
        assertThat(m.prefetchCancelledBytes()).isEqualTo(256 * 1024);
        assertThat(inflight.current()).isEqualTo(0);
        // Nothing was cached (never completed).
        assertThat(cache.findCovering(OBJECT_ID, 0, 256 * 1024)).isNull();
    }

    @Test
    void failedPrefetchDoesNotLeakOrCorruptSubsequentDemand() {
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore().define(KEY, SIZE).failAll(true);
        Prefetcher pf = newPrefetcher(store);

        pf.schedule(Collections.singletonList(new FetchRange(0, 256 * 1024)));
        // The speculative GET failed: nothing cached, budget/inflight released.
        assertThat(inflight.current()).isEqualTo(0);
        assertThat(cache.findCovering(OBJECT_ID, 0, 256 * 1024)).isNull();

        store.failAll(false);
        byte[] got = pf.demand(2048, 2048 + 4096);
        assertThat(got).isEqualTo(InMemoryAsyncObjectStore.expected(2048, 2048 + 4096));
    }

    @Test
    void oversizedPrefetchIsSkipped() {
        InMemoryAsyncObjectStore store = new InMemoryAsyncObjectStore().define(KEY, SIZE);
        Prefetcher pf = newPrefetcher(store);

        // Range larger than maxSingleFetch must not be prefetched.
        pf.schedule(Collections.singletonList(new FetchRange(0, MAX_SINGLE_FETCH + 1)));
        assertThat(store.getCount()).isEqualTo(0);
    }
}
