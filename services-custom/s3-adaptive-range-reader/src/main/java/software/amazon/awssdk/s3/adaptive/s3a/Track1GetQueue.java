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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.s3.adaptive.internal.RuntimeConfig;
import software.amazon.awssdk.s3.adaptive.internal.budget.ConcurrencyLimiter;
import software.amazon.awssdk.s3.adaptive.internal.budget.InflightLimiter;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * D2 queue: sync callers enqueue and wait; workers issue via {@link S3AsyncClient}.
 * When D4 is on, a wait window collects same-object neighbours and {@link RangeMerger}
 * turns them into one GET, then slices the body back.
 */
@SdkInternalApi
public final class Track1GetQueue {

    private final RuntimeConfig config;
    private final LinkEstimator link;
    private final ConcurrencyLimiter concurrency;
    private final InflightLimiter inflight;
    private final Track1S3aStats stats;
    private final LinkedBlockingQueue<Ticket> queue = new LinkedBlockingQueue<Ticket>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final List<Thread> workers = new ArrayList<Thread>();

    public Track1GetQueue(RuntimeConfig config,
                          LinkEstimator link,
                          ConcurrencyLimiter concurrency,
                          InflightLimiter inflight,
                          Track1S3aStats stats) {
        this.config = config;
        this.link = link;
        this.concurrency = concurrency;
        this.inflight = inflight;
        this.stats = stats;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        // One dispatcher collects the wait window. In-flight HTTP is capped by
        // ConcurrencyLimiter; extra take-loops would split a mergeable batch.
        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                loop();
            }
        }, "track1-d2-dispatcher");
        t.setDaemon(true);
        workers.add(t);
        t.start();
    }

    public CompletableFuture<Fetched> submit(S3AsyncClient async, GetObjectRequest request,
                                             long start, long endInclusive) {
        Ticket ticket = new Ticket(async, request, start, endInclusive);
        if (closed.get() || !queue.offer(ticket)) {
            ticket.future.completeExceptionally(new IllegalStateException("track1 d2 queue unavailable"));
            return ticket.future;
        }
        stats.queueSubmitted();
        return ticket.future;
    }

    public void close() {
        closed.set(true);
        for (Thread t : workers) {
            t.interrupt();
        }
        workers.clear();
        Ticket leftover;
        while ((leftover = queue.poll()) != null) {
            leftover.future.completeExceptionally(new IllegalStateException("track1 d2 queue closed"));
        }
    }

    private void loop() {
        while (!closed.get() && !Thread.currentThread().isInterrupted()) {
            try {
                Ticket first = queue.take();
                List<Ticket> batch = new ArrayList<Ticket>();
                batch.add(first);
                queue.drainTo(batch);
                long waitNs = TimeUnit.MICROSECONDS.toNanos(config.d2WaitWindowMicros());
                if (config.d4Enabled() && waitNs > 0) {
                    long deadline = System.nanoTime() + waitNs;
                    while (true) {
                        long remain = deadline - System.nanoTime();
                        if (remain <= 0) {
                            break;
                        }
                        Ticket extra = queue.poll(remain, TimeUnit.NANOSECONDS);
                        if (extra == null) {
                            break;
                        }
                        batch.add(extra);
                    }
                    queue.drainTo(batch);
                }
                dispatch(batch);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable ignored) {
                // A worker fault must not kill the pool; tickets already in a
                // bad batch should have been completed by dispatch.
            }
        }
    }

    private void dispatch(List<Ticket> batch) {
        stats.queueBatch(batch.size());
        long dispatchedAt = System.nanoTime();
        for (Ticket ticket : batch) {
            stats.queueWait(dispatchedAt - ticket.enqueuedAt);
        }
        if (!config.d4Enabled() || batch.size() == 1) {
            for (Ticket t : batch) {
                issueExact(t);
            }
            return;
        }
        Map<String, List<Ticket>> byObject = new LinkedHashMap<String, List<Ticket>>();
        for (Ticket t : batch) {
            String id = Track1RangeCache.objectId(t.request, null);
            List<Ticket> group = byObject.get(id);
            if (group == null) {
                group = new ArrayList<Ticket>();
                byObject.put(id, group);
            }
            group.add(t);
        }
        for (List<Ticket> same : byObject.values()) {
            stats.sameObjectGroup(same.size());
            mergeAndIssue(same);
        }
    }

    private void mergeAndIssue(List<Ticket> same) {
        List<RangeMerger.Span> spans = new ArrayList<RangeMerger.Span>(same.size());
        for (Ticket t : same) {
            spans.add(new RangeMerger.Span(t.start, t.endInclusive));
        }
        long gStar = link.gStarBytes();
        List<RangeMerger.Group> groups = RangeMerger.group(
            spans, gStar, config.maxSingleFetchBytes(), config.d4MaxWasteRatio());
        for (RangeMerger.Group group : groups) {
            List<Ticket> members = new ArrayList<Ticket>(group.members.size());
            for (int idx : group.members) {
                members.add(same.get(idx));
            }
            if (members.size() == 1 || !inflight.tryReserve(group.length())) {
                if (members.size() > 1) {
                    stats.mergeableGroup();
                    stats.mergeBudgetFallback();
                }
                for (Ticket t : members) {
                    issueExact(t);
                }
                continue;
            }
            stats.mergeableGroup();
            stats.merged(members.size(), group.wasteBytes);
            issueMerged(members, group);
        }
    }

    private void issueExact(Ticket ticket) {
        issue(ticket.async, ticket.request, ticket.start, ticket.endInclusive, ticket);
    }

    private void issueMerged(List<Ticket> members, RangeMerger.Group group) {
        Ticket first = members.get(0);
        GetObjectRequest merged = first.request.toBuilder()
                                               .range("bytes=" + group.start + "-" + group.endInclusive)
                                               .build();
        long t0 = System.nanoTime();
        concurrency.acquire();
        stats.remoteGet();
        first.async.getObject(merged, AsyncResponseTransformer.toBytes())
                   .whenComplete((resp, err) -> {
                       concurrency.release();
                       inflight.release(group.length());
                       long latency = System.nanoTime() - t0;
                       if (err != null) {
                           Throwable cause = unwrap(err);
                           for (Ticket t : members) {
                               t.future.completeExceptionally(cause);
                           }
                           return;
                       }
                       byte[] body = resp.asByteArrayUnsafe();
                       link.record(body.length, latency);
                       GetObjectResponse meta = resp.response();
                       for (Ticket t : members) {
                           int from = (int) (t.start - group.start);
                           int len = (int) (t.endInclusive - t.start + 1);
                           if (from < 0 || from + len > body.length) {
                               t.future.completeExceptionally(
                                   new IllegalStateException("merged GET shorter than requested slice"));
                               continue;
                           }
                           byte[] slice = new byte[len];
                           System.arraycopy(body, from, slice, 0, len);
                           t.future.complete(new Fetched(slice, meta));
                       }
                   });
    }

    private void issue(S3AsyncClient async, GetObjectRequest request, long start, long endInclusive,
                       Ticket ticket) {
        GetObjectRequest ranged = request.toBuilder()
                                         .range("bytes=" + start + "-" + endInclusive)
                                         .build();
        long t0 = System.nanoTime();
        concurrency.acquire();
        stats.remoteGet();
        long reserved = endInclusive - start + 1;
        boolean held = inflight.tryReserve(reserved);
        async.getObject(ranged, AsyncResponseTransformer.toBytes())
             .whenComplete((resp, err) -> {
                 concurrency.release();
                 if (held) {
                     inflight.release(reserved);
                 }
                 long latency = System.nanoTime() - t0;
                 if (err != null) {
                     ticket.future.completeExceptionally(unwrap(err));
                     return;
                 }
                 byte[] body = resp.asByteArrayUnsafe();
                 link.record(body.length, latency);
                 ticket.future.complete(new Fetched(body, resp.response()));
             });
    }

    private static Throwable unwrap(Throwable err) {
        if (err instanceof CompletionException && err.getCause() != null) {
            return err.getCause();
        }
        return err;
    }

    static final class Ticket {
        final S3AsyncClient async;
        final GetObjectRequest request;
        final long start;
        final long endInclusive;
        final long enqueuedAt = System.nanoTime();
        final CompletableFuture<Fetched> future = new CompletableFuture<Fetched>();

        Ticket(S3AsyncClient async, GetObjectRequest request, long start, long endInclusive) {
            this.async = async;
            this.request = request;
            this.start = start;
            this.endInclusive = endInclusive;
        }
    }

    static final class Fetched {
        final byte[] bytes;
        final GetObjectResponse response;

        Fetched(byte[] bytes, GetObjectResponse response) {
            this.bytes = bytes;
            this.response = response;
        }
    }
}
