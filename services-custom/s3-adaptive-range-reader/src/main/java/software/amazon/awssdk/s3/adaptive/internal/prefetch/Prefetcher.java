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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.internal.budget.ConcurrencyLimiter;
import software.amazon.awssdk.s3.adaptive.internal.budget.InflightLimiter;
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache;
import software.amazon.awssdk.s3.adaptive.internal.cache.CachedBlock;
import software.amazon.awssdk.s3.adaptive.internal.exec.FetchRange;
import software.amazon.awssdk.s3.adaptive.internal.io.AsyncObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectChangedException;
import software.amazon.awssdk.s3.adaptive.internal.metrics.MetricsRecorder;

/**
 * Manages asynchronous IO for one reader/stream: it deduplicates overlapping fetches (a demand read reuses an
 * in-flight speculative fetch instead of issuing a second GET), schedules speculative look-ahead within the app's
 * concurrency / in-flight caps, cancels no-longer-needed speculation on a {@code seek}, and falls back to a fresh
 * demand GET when a speculative fetch fails. Completed speculative bytes are inserted into the shared per-app
 * {@link AppCache}; blocks it filled are remembered so the reader can attribute "useful prefetch".
 *
 * <p>Bound to a single object version (one reader). The in-flight registry is guarded by this instance's monitor;
 * blocking {@code join()}s are always performed without holding the lock. Prefetch completions run on async IO
 * threads and record into the (thread-safe) {@link MetricsRecorder}.
 */
@SdkInternalApi
public final class Prefetcher {

    private final AsyncObjectStore store;
    private final String key;
    private final String objectId;
    private final String versionToken;
    private final long objectSize;
    private final long maxSingleFetchBytes;
    private final AppCache cache;
    private final MetricsRecorder metrics;
    private final ConcurrencyLimiter concurrency;
    private final InflightLimiter inflight;

    private final Map<Long, InFlight> registry = new HashMap<>();
    private final Set<Long> prefetchedStarts = ConcurrentHashMap.newKeySet();

    public Prefetcher(AsyncObjectStore store, String key, String objectId, String versionToken, long objectSize,
                      long maxSingleFetchBytes, AppCache cache, MetricsRecorder metrics,
                      ConcurrencyLimiter concurrency, InflightLimiter inflight) {
        this.store = store;
        this.key = key;
        this.objectId = objectId;
        this.versionToken = versionToken;
        this.objectSize = objectSize;
        this.maxSingleFetchBytes = maxSingleFetchBytes;
        this.cache = cache;
        this.metrics = metrics;
        this.concurrency = concurrency;
        this.inflight = inflight;
    }

    /**
     * Fetch {@code [start, endExclusive)}, caching the result. If a speculative fetch already covers the range, block
     * on it (deduplicated, and counted as useful prefetch); otherwise issue a blocking demand GET.
     */
    public byte[] demand(long start, long endExclusive) {
        InFlight hit;
        synchronized (this) {
            hit = coveringInFlight(start, endExclusive);
        }
        if (hit != null) {
            try {
                byte[] full = join(hit.future);
                metrics.recordPrefetchUseful(endExclusive - start);
                return slice(full, (int) (start - hit.start), (int) (endExclusive - start));
            } catch (ObjectChangedException e) {
                throw e;
            } catch (RuntimeException e) {
                // Speculative fetch failed: fall through to a fresh demand GET.
            }
        }
        return demandFetch(start, endExclusive);
    }

    private byte[] demandFetch(long start, long endExclusive) {
        concurrency.acquire();
        byte[] data;
        try {
            data = join(store.getRange(key, start, endExclusive, versionToken));
        } finally {
            concurrency.release();
        }
        metrics.recordRemoteFetch(data.length);
        cache.put(objectId, start, data);
        return data;
    }

    /**
     * Schedule speculative prefetch of the given ranges, skipping any that are already cached, already in flight, too
     * large to speculate, or that would exceed the concurrency / in-flight caps.
     */
    public void schedule(List<FetchRange> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            return;
        }
        for (FetchRange range : ranges) {
            long start = Math.max(0L, range.start());
            long end = Math.min(objectSize, range.endExclusive());
            if (end <= start) {
                continue;
            }
            long bytes = end - start;
            if (bytes > maxSingleFetchBytes) {
                continue;
            }
            scheduleOne(start, end, bytes);
        }
    }

    private void scheduleOne(long start, long end, long bytes) {
        synchronized (this) {
            if (registry.containsKey(start) || coveringInFlight(start, end) != null) {
                return;
            }
            CachedBlock cached = cache.findCovering(objectId, start, end);
            if (cached != null) {
                return;
            }
            if (!inflight.tryReserve(bytes)) {
                return;
            }
            if (!concurrency.tryAcquire()) {
                inflight.release(bytes);
                return;
            }
            InFlight inf = new InFlight(start, end, bytes);
            registry.put(start, inf);
            try {
                inf.future = store.getRange(key, start, end, versionToken);
            } catch (RuntimeException e) {
                registry.remove(start, inf);
                concurrency.release();
                inflight.release(bytes);
                return;
            }
            inf.future.whenComplete((data, err) -> onPrefetchDone(inf, data, err));
        }
    }

    private void onPrefetchDone(InFlight inf, byte[] data, Throwable err) {
        synchronized (this) {
            registry.remove(inf.start, inf);
        }
        concurrency.release();
        inflight.release(inf.bytes);
        if (err == null && data != null) {
            metrics.recordPrefetchFetch(data.length);
            if (cache.put(objectId, inf.start, data)) {
                prefetchedStarts.add(inf.start);
            }
        }
    }

    /**
     * Cancel all in-flight speculative fetches (e.g. on a {@code seek} away). Their bytes are counted as cancelled;
     * resources are released by the cancellation completion callback.
     */
    public void cancelSpeculative() {
        List<InFlight> victims;
        synchronized (this) {
            victims = new ArrayList<>(registry.values());
        }
        for (InFlight inf : victims) {
            metrics.recordPrefetchCancelled(inf.bytes);
            if (inf.future != null) {
                inf.future.cancel(true);
            }
        }
    }

    public void close() {
        cancelSpeculative();
    }

    /**
     * Whether the given block start was filled by a speculative prefetch (best-effort provenance for "useful" attribution).
     */
    public boolean wasPrefetched(long blockStart) {
        return prefetchedStarts.contains(blockStart);
    }

    private InFlight coveringInFlight(long start, long end) {
        for (InFlight inf : registry.values()) {
            if (inf.start <= start && inf.end >= end) {
                return inf;
            }
        }
        return null;
    }

    private static byte[] slice(byte[] src, int from, int len) {
        byte[] out = new byte[len];
        System.arraycopy(src, from, out, 0, len);
        return out;
    }

    private static byte[] join(CompletableFuture<byte[]> future) {
        try {
            return future.join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw e;
        }
    }

    private static final class InFlight {
        private final long start;
        private final long end;
        private final long bytes;
        private volatile CompletableFuture<byte[]> future;

        private InFlight(long start, long end, long bytes) {
            this.start = start;
            this.end = end;
            this.bytes = bytes;
        }
    }
}
