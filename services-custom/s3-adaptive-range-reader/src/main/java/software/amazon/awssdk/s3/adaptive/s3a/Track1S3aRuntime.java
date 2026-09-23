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

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicReference;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.internal.RuntimeConfig;
import software.amazon.awssdk.s3.adaptive.internal.budget.AppBudgetLease;
import software.amazon.awssdk.s3.adaptive.internal.budget.ConcurrencyLimiter;
import software.amazon.awssdk.s3.adaptive.internal.budget.GlobalBudget;
import software.amazon.awssdk.s3.adaptive.internal.budget.InflightLimiter;
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * One Spark-process Track 1 runtime. Cold-starts with the JVM; D1 cache is
 * shared across the 43 queries of a run. Default config has every dimension
 * off, so constructing this is a no-op until {@code -Dtrack1.d1/d2/d4} is set.
 */
@SdkInternalApi
public final class Track1S3aRuntime {

    private static final String APP = "spark";
    private static final AtomicReference<Track1S3aRuntime> SHARED =
        new AtomicReference<Track1S3aRuntime>();

    private final RuntimeConfig config;
    private final Track1S3aStats stats = new Track1S3aStats();
    private final AppCache appCache;
    private final Track1RangeCache cache;
    private final D1SoftController controller;
    private final LinkEstimator link;
    private final Track1GetQueue queue;
    private final Track1GetPipeline pipeline;
    private final AtomicReference<S3AsyncClient> async = new AtomicReference<S3AsyncClient>();
    private final AtomicReference<S3AsyncClient> ownedAsync = new AtomicReference<S3AsyncClient>();
    private final AtomicReference<S3Client> sync = new AtomicReference<S3Client>();

    public Track1S3aRuntime(RuntimeConfig config) {
        this.config = config;
        GlobalBudget budget = new GlobalBudget(config.globalCacheBytes());
        AppBudgetLease lease = new AppBudgetLease(budget, APP);
        this.appCache = new AppCache(lease);
        if (config.d1Adaptive()) {
            this.appCache.setReplacementPolicy(AppCache.ReplacementPolicy.WTINYLFU);
        }
        budget.register(APP, config.perAppReservedBytes(), appCache);
        this.cache = new Track1RangeCache(appCache, config.d1BlockBytes(), stats, config.d1Profile(),
                                           config.d1ZeroCopy(), config.d1SharedBacking());
        this.controller = new D1SoftController(config, appCache);
        this.link = new LinkEstimator(config.maxSingleFetchBytes());
        ConcurrencyLimiter concurrency = new ConcurrencyLimiter(config.maxConcurrentGets());
        InflightLimiter inflight = new InflightLimiter(Math.max(
            config.perAppInflightBytes(), config.maxSingleFetchBytes()));
        this.queue = new Track1GetQueue(config, link, concurrency, inflight, stats);
        if (config.d2Enabled()) {
            this.queue.start();
        }
        this.pipeline = new Track1GetPipeline(this);
    }

    public static Track1S3aRuntime shared() {
        Track1S3aRuntime current = SHARED.get();
        if (current != null) {
            return current;
        }
        Track1S3aRuntime created = new Track1S3aRuntime(RuntimeConfig.fromSystemProperties());
        if (SHARED.compareAndSet(null, created)) {
            return created;
        }
        created.close();
        return SHARED.get();
    }

    public static Track1S3aRuntime resetShared(RuntimeConfig config) {
        Track1S3aRuntime next = new Track1S3aRuntime(config);
        Track1S3aRuntime prev = SHARED.getAndSet(next);
        if (prev != null) {
            prev.close();
        }
        return next;
    }

    public RuntimeConfig config() {
        return config;
    }

    public Track1S3aStats stats() {
        return stats;
    }

    public Track1RangeCache cache() {
        return cache;
    }

    public D1SoftController controller() {
        return controller;
    }

    public AppCache appCache() {
        return appCache;
    }

    public LinkEstimator link() {
        return link;
    }

    public Track1GetQueue queue() {
        return queue;
    }

    public Track1GetPipeline pipeline() {
        return pipeline;
    }

    public void registerSync(S3Client client) {
        if (client != null) {
            sync.compareAndSet(null, client);
        }
    }

    public void registerAsync(S3AsyncClient client) {
        if (client != null) {
            async.compareAndSet(null, client);
        }
    }

    /**
     * Register an async client created solely for the D2/D4 path. S3A commonly
     * creates only a sync client, so there is otherwise no async client for the
     * queue to use. The runtime owns and closes this client.
     *
     * @return true when the client became the process async client
     */
    public boolean registerOwnedAsync(S3AsyncClient client) {
        if (client == null || !async.compareAndSet(null, client)) {
            return false;
        }
        ownedAsync.set(client);
        return true;
    }

    public S3Client syncClient() {
        return sync.get();
    }

    public S3AsyncClient asyncClient() {
        return async.get();
    }

    public boolean dimensionsEnabled() {
        return config.d1Enabled() || config.d2Enabled() || config.d4Enabled();
    }

    /**
     * JSON snapshot for the matrix runner. Also written to
     * {@code track1.s3a.probe.dir/track1-s3a-stats.json} when that dir is set.
     */
    public String snapshotJson() {
        stats.sampleHeap();
        stats.sampleGc();
        String json = "{"
            + "\"d1\":" + config.d1Enabled()
            + ",\"d2\":" + config.d2Enabled()
            + ",\"d4\":" + config.d4Enabled()
            + ",\"async_client_available\":" + (asyncClient() != null)
            + ",\"d2_wait_us\":" + config.d2WaitWindowMicros()
            + ",\"cache_hits\":" + stats.cacheHits()
            + ",\"cache_misses\":" + stats.cacheMisses()
            + ",\"cache_useful_bytes\":" + stats.cacheUsefulBytes()
            + ",\"cached_bytes\":" + cache.cachedBytes()
            + ",\"peak_cached_bytes\":" + appCache.peakCachedBytes()
            + ",\"evicted_bytes\":" + appCache.evictedBytes()
            + ",\"evicted_blocks\":" + appCache.evictedBlocks()
            + ",\"d1_admitted_blocks\":" + appCache.admittedBlocks()
            + ",\"d1_rejected_blocks\":" + appCache.rejectedBlocks()
            + ",\"d1_ghost_hits\":" + appCache.evictionGhostHits()
            + ",\"d1_reject_ghost_hits\":" + appCache.rejectGhostHits()
            + ",\"d1_reject_request_ghost_hits\":" + appCache.rejectRequestGhostHits()
            + ",\"d1_eviction_ghost_hits\":" + appCache.evictionGhostHits()
            + ",\"d1_victim_plan_count\":" + appCache.lastVictimCount()
            + ",\"d1_window_bytes\":" + appCache.windowBytes()
            + ",\"d1_probation_bytes\":" + appCache.probationBytes()
            + ",\"d1_protected_bytes\":" + appCache.protectedBytes()
            + ",\"d1_metadata_bytes\":" + appCache.sketchBytes()
            + ",\"remote_gets\":" + stats.remoteGets()
            + ",\"merged_gets\":" + stats.mergedGets()
            + ",\"merged_members\":" + stats.mergedMembers()
            + ",\"wasted_bytes\":" + stats.wastedBytes()
            + ",\"queue_wait_ns\":" + stats.queueWaitNanos()
            + ",\"queue_submissions\":" + stats.queueSubmissions()
            + ",\"queue_batches\":" + stats.queueBatches()
            + ",\"queue_batch_tickets\":" + stats.queueBatchTickets()
            + ",\"queue_singleton_batches\":" + stats.queueSingletonBatches()
            + ",\"same_object_multi_ticket_groups\":" + stats.sameObjectMultiTicketGroups()
            + ",\"mergeable_groups\":" + stats.mergeableGroups()
            + ",\"merge_budget_fallbacks\":" + stats.mergeBudgetFallbacks()
            + ",\"max_batch_size\":" + stats.maxBatchSize()
            + ",\"max_same_object_group_size\":" + stats.maxSameObjectGroupSize()
            + ",\"fallbacks\":" + stats.fallbacks()
            + ",\"d1_admit_max_bytes\":" + config.d1AdmitMaxBytes()
            + "," + controller.snapshotFragment()
            + ",\"admit_rejected\":" + stats.admitRejected()
            + ",\"teed_gets\":" + stats.teedGets()
            + ",\"teed_bytes\":" + stats.teedBytes()
            + ",\"d1_profile\":" + config.d1Profile()
            + ",\"d1_zero_copy\":" + config.d1ZeroCopy()
            + ",\"d1_doorkeeper\":" + config.d1Doorkeeper()
            + ",\"d1_shared_backing\":" + config.d1SharedBacking()
            + ",\"d1_cache_lookup_ns\":" + stats.cacheLookupNanos()
            + ",\"d1_cache_hit_copy_ns\":" + stats.cacheHitCopyNanos()
            + ",\"d1_cache_hit_copy_bytes\":" + stats.cacheHitCopyBytes()
            + ",\"d1_cache_stitch_ns\":" + stats.cacheStitchNanos()
            + ",\"d1_cache_put_ns\":" + stats.cachePutNanos()
            + ",\"d1_tee_copy_ns\":" + stats.teeCopyNanos()
            + ",\"d1_tee_copy_bytes\":" + stats.teeCopyBytes()
            + ",\"d1_rejected_payload_copy_bytes\":" + stats.rejectedPayloadCopyBytes()
            + ",\"gc_ms\":" + stats.gcTimeMs()
            + ",\"gc_count\":" + stats.gcCount()
            + ",\"peak_heap_bytes\":" + stats.peakHeapBytes()
            + ",\"g_star_bytes\":" + link.gStarBytes()
            + ",\"link_samples\":" + link.samples()
            + ",\"rtt_ns\":" + (long) link.rttNanos()
            + ",\"bw_bps\":" + (long) link.bwBytesPerSec()
            + "," + appCache.remoteCostSnapshotFragment()
            + "," + appCache.requestSizeFragment()
            + "}";
        writeSnapshot(json);
        return json;
    }

    private static void writeSnapshot(String json) {
        String dir = System.getProperty("track1.s3a.probe.dir",
                                        System.getenv("TRACK1_S3A_PROBE_DIR"));
        if (dir == null || dir.isEmpty()) {
            return;
        }
        try {
            Path folder = Paths.get(dir);
            Files.createDirectories(folder);
            Path file = folder.resolve("track1-s3a-stats.json");
            Writer w = new OutputStreamWriter(Files.newOutputStream(file), StandardCharsets.UTF_8);
            try {
                w.write(json);
            } finally {
                w.close();
            }
        } catch (Exception ignored) {
            // Losing a stats file must not fail the run being measured.
        }
    }

    public void close() {
        queue.close();
        S3AsyncClient client = ownedAsync.getAndSet(null);
        if (client != null) {
            async.compareAndSet(client, null);
            try {
                client.close();
            } catch (Throwable ignored) {
                // Closing an optimization-owned client must not mask shutdown.
            }
        }
        stats.reset();
    }
}
