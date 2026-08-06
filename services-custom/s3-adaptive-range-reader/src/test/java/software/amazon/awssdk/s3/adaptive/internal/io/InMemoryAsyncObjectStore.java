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

package software.amazon.awssdk.s3.adaptive.internal.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Deterministic {@link AsyncObjectStore} test double. Supports three completion modes so async behaviour is
 * reproducible:
 * <ul>
 *   <li><b>immediate</b> (default): the future is already completed on return;</li>
 *   <li><b>executor</b> ({@link #executor(ExecutorService)}): completion runs on the given pool (optionally after
 *   {@link #latencyMillis(long)});</li>
 *   <li><b>manual</b> ({@link #manual(boolean)}): futures stay pending until {@link #completeAll()} is called, so
 *   tests can drive in-flight dedup / cancellation / concurrency deterministically.</li>
 * </ul>
 * Bytes are a deterministic function of absolute position ({@link #byteAt(long)}) so huge objects need no allocation.
 * Records GET count, total bytes, and the peak number of concurrently in-flight GETs.
 */
public final class InMemoryAsyncObjectStore implements AsyncObjectStore {

    private final Map<String, Long> sizes = new ConcurrentHashMap<>();
    private final List<Pending> pending = new ArrayList<>();

    private final AtomicInteger inFlight = new AtomicInteger();
    private final AtomicInteger peakInFlight = new AtomicInteger();
    private final AtomicInteger getCount = new AtomicInteger();
    private final AtomicLong totalGetBytes = new AtomicLong();

    private volatile ExecutorService executor;
    private volatile long latencyMillis;
    private volatile long rttNanos;
    private volatile double bandwidthMiBps;
    private volatile boolean manual;
    private volatile boolean failAll;
    private volatile boolean objectChanged;

    public InMemoryAsyncObjectStore define(String key, long size) {
        sizes.merge(key, size, Math::max);
        return this;
    }

    public InMemoryAsyncObjectStore executor(ExecutorService executor) {
        this.executor = executor;
        return this;
    }

    public InMemoryAsyncObjectStore latencyMillis(long latencyMillis) {
        this.latencyMillis = latencyMillis;
        return this;
    }

    /**
     * Inject a size-aware per-GET {@code rtt + bytes/BW} delay (see {@link SimLatency}); adds to {@link #latencyMillis}.
     */
    public InMemoryAsyncObjectStore latency(long rttNanos, double bandwidthMiBps) {
        this.rttNanos = rttNanos;
        this.bandwidthMiBps = bandwidthMiBps;
        return this;
    }

    public InMemoryAsyncObjectStore manual(boolean manual) {
        this.manual = manual;
        return this;
    }

    public InMemoryAsyncObjectStore failAll(boolean failAll) {
        this.failAll = failAll;
        return this;
    }

    /**
     * Simulate a mid-read overwrite: subsequent ranged GETs fail with {@link ObjectChangedException}.
     */
    public InMemoryAsyncObjectStore objectChanged(boolean objectChanged) {
        this.objectChanged = objectChanged;
        return this;
    }

    /**
     * Deterministic pseudo-random byte for an absolute object position (identical scheme to {@code GeneratedObjectStore}).
     */
    public static byte byteAt(long pos) {
        long x = (pos + 1) * 0x9E3779B97F4A7C15L;
        x ^= x >>> 29;
        x *= 0xBF58476D1CE4E5B9L;
        x ^= x >>> 32;
        return (byte) x;
    }

    public static byte[] expected(long start, long endExclusive) {
        int len = (int) (endExclusive - start);
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = byteAt(start + i);
        }
        return out;
    }

    @Override
    public ObjectMeta head(String key) {
        Long size = sizes.get(key);
        if (size == null) {
            throw new IllegalArgumentException("no such object: " + key);
        }
        return new ObjectMeta(size, key + "#v1");
    }

    @Override
    public CompletableFuture<byte[]> getRange(String key, long start, long endExclusive, String expectedVersionToken) {
        Long size = sizes.get(key);
        if (size == null) {
            throw new IllegalArgumentException("no such object: " + key);
        }
        if (start < 0 || endExclusive > size || endExclusive < start) {
            throw new IndexOutOfBoundsException("bad range [" + start + "," + endExclusive + ") size=" + size);
        }
        getCount.incrementAndGet();
        if (objectChanged) {
            CompletableFuture<byte[]> failed = new CompletableFuture<>();
            failed.completeExceptionally(new ObjectChangedException("object " + key + " changed during read"));
            return failed;
        }
        int len = (int) (endExclusive - start);
        totalGetBytes.addAndGet(len);

        int now = inFlight.incrementAndGet();
        peakInFlight.accumulateAndGet(now, Math::max);

        CompletableFuture<byte[]> future = new CompletableFuture<>();
        future.whenComplete((r, e) -> inFlight.decrementAndGet());

        Runnable task = () -> {
            SimLatency.sleepNanos(latencyMillis * 1_000_000L + SimLatency.fetchNanos(rttNanos, bandwidthMiBps, len));
            if (failAll) {
                future.completeExceptionally(new RuntimeException("injected failure for " + key));
            } else {
                future.complete(expected(start, endExclusive));
            }
        };

        if (manual) {
            synchronized (pending) {
                pending.add(new Pending(future, task));
            }
        } else if (executor != null) {
            executor.submit(task);
        } else {
            task.run();
        }
        return future;
    }

    /**
     * Complete (run) every pending manual fetch that has not been cancelled.
     */
    public void completeAll() {
        List<Pending> snapshot;
        synchronized (pending) {
            snapshot = new ArrayList<>(pending);
            pending.clear();
        }
        for (Pending p : snapshot) {
            if (!p.future.isDone()) {
                p.task.run();
            }
        }
    }

    public int pendingCount() {
        synchronized (pending) {
            return pending.size();
        }
    }

    public int getCount() {
        return getCount.get();
    }

    public long totalGetBytes() {
        return totalGetBytes.get();
    }

    public int peakInFlight() {
        return peakInFlight.get();
    }

    public int inFlight() {
        return inFlight.get();
    }

    private static final class Pending {
        private final CompletableFuture<byte[]> future;
        private final Runnable task;

        private Pending(CompletableFuture<byte[]> future, Runnable task) {
            this.future = future;
            this.task = task;
        }
    }
}
