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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.ToDoubleFunction;
import java.util.function.ToLongFunction;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.internal.AdaptiveRangeReaderImpl;
import software.amazon.awssdk.s3.adaptive.internal.PassthroughRangeReader;
import software.amazon.awssdk.s3.adaptive.internal.io.AsyncObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.io.GeneratedObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.io.InMemoryAsyncObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.io.S3ClientObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.io.SimLatency;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * A manually-run, end-to-end system benchmark for the S2 adaptive range reader. It is intentionally NOT named
 * {@code *Test}, so Surefire does not pick it up during {@code mvn verify}; run it explicitly:
 *
 * <pre>
 *   mvn -q -pl services-custom/s3-adaptive-range-reader \
 *       -Djapicmp.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true \
 *       -Dtest=AdaptiveReaderSystemBenchmark test
 * </pre>
 *
 * <p>Optional system properties:
 * <ul>
 *   <li>{@code -Ds3arr.trace=/abs/path.csv} trace to replay (default: repo {@code traces/tpch_sf1_full.csv},
 *       falling back to the bundled {@code /traces/tpch_slice.csv} resource).</li>
 *   <li>{@code -Ds3arr.label=S2} label recorded in the CSV row.</li>
 *   <li>{@code -Ds3arr.backend=synthetic} (default) replays deterministic in-memory objects. Set
 *       {@code -Ds3arr.backend=s3} to use the bucket and credentials supplied by {@link MinioEnvironment}; the
 *       trace objects are HEAD-checked before replay.</li>
 *   <li>{@code -Ds3arr.warmup=2} / {@code -Ds3arr.iters=5} timing iterations.</li>
 *   <li>{@code -Ds3arr.cacheBudgetMiB=64} global cache budget (per-app reserve = global / apps, shared by S2 and S3);
 *       {@code -Ds3arr.prefetchBlockMiB=1} planner block size; {@code -Ds3arr.maxFetchMiB} is an optional
 *       single-fetch ceiling (default: enough for two blocks plus the largest trace read).</li>
 *   <li>{@code -Ds3arr.apps=1} number of isolated apps (keys are hashed across them; used by BOTH S2 and S3);
 *       {@code -Ds3arr.prefetchDepth=1} S3 speculative look-ahead depth (S2 is always 0).</li>
 *   <li>{@code -Ds3arr.forcePolicy=s3a_prefetch} runs only a fixed-policy sweep row. Alternatively,
 *       {@code -Ds3arr.oracleMap=tpch:s3a_random,clickbench:template_locality} runs the measured prefix oracle.
 *       {@code -Ds3arr.selector=passthrough|template_auto|decision_tree} runs one ordinary mode at the configured work
 *       point. All three modes emit per-prefix rows in {@code results-v2.csv}.</li>
 *   <li>{@code -Ds3arr.rttMs=0} fixed per-GET round-trip latency (ms), {@code -Ds3arr.bwMiBps=0} transfer bandwidth
 *       (0 = unlimited), {@code -Ds3arr.thinkMs=0} synthetic per-read compute gap (ms). All three form the remote-read
 *       cost model injected as REAL delays into every GET (see {@code SimLatency}); with them the reported ns/op,
 *       p50-p99 and "IO latency sum" reflect network cost, and async prefetch's RTT-hiding becomes measurable (it needs
 *       a non-zero think gap to overlap). Leaving them 0 keeps the fast zero-RTT IO-shape comparison. When latency is
 *       on, warmup/iters default to 0/1 (each pass is ~reads*(rtt+think) long).</li>
 * </ul>
 *
 * <p>Three modes are compared on a FAIR budget: {@code passthrough} (flag off, raw no-cache demand),
 * {@code S2(appID,d=0)} and {@code S3(appID,d=N)}. Crucially S2 and S3 both run on the SAME
 * {@link AdaptiveReaderRuntime} - same appID organisation and the same work-conserving global budget split across
 * the same number of apps - and differ ONLY by prefetch depth (S2 = 0). So "S3 vs S2" isolates the effect of async
 * prefetch alone, instead of conflating it with a different cache/budget model. The report prints prefetch
 * useful/wasted bytes and the per-app {@link RuntimeMetrics} breakdown proving isolation.
 *
 * <p>By default each mode replays the trace over a synthetic store (bytes are a deterministic function of position, so
 * 100+ MiB objects need no allocation and every read is verified against the expected bytes once):
 * {@link GeneratedObjectStore} for passthrough and {@link InMemoryAsyncObjectStore} for the async S2/S3 runtime path.
 * {@code backend=s3} instead uses the existing S3 sync/async ObjectStore adapters against MinIO or another compatible
 * endpoint. It reports IO-efficiency metrics (GET count, read amplification, cache hit rate, per-policy counts) and
 * wall-clock ns/op. Synthetic wall-clock excludes real network latency; real-S3 wall-clock is the final latency
 * measurement.
 *
 * <p>Reusable after S3: it drives only the public {@link AdaptiveRangeReader} contract and the ObjectStore SPI, so
 * once async prefetch lands the same harness reports the new GET/amplification/latency profile under the same label.
 */
class AdaptiveReaderSystemBenchmark {

    private static final double MIB = 1024.0 * 1024.0;

    private int warmup;
    private int iters;
    private long cacheBudget;
    private long maxFetch;
    private long prefetchBlockSize;
    private long rttNanos;
    private double bwMiBps;
    private long thinkNanos;
    private ExecutorService ioPool;
    private Backend backend;
    private String bucket;
    private S3Client realSyncClient;
    private S3AsyncClient realAsyncClient;

    private enum Backend {
        SYNTHETIC,
        S3
    }

    private boolean latencyOn() {
        return rttNanos > 0 || bwMiBps > 0;
    }

    private boolean realBackend() {
        return backend == Backend.S3;
    }

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

    private static final class Result {
        private String mode;
        private long totalReads;
        private long logicalBytes;
        private long remoteGets;
        private long remoteBytes;
        private long policySwitches;
        private long fallbackReads;
        private long demandClampReads;
        private long cacheHitReads;
        private long prefetchGets;
        private long prefetchBytes;
        private long prefetchUsefulBytes;
        private long prefetchWastedBytes;
        private long prefetchCancelledBytes;
        private String isolationSummary = "";
        private Map<PolicyName, Long> policyReads = new EnumMap<>(PolicyName.class);
        private Map<String, Result> prefixResults = new LinkedHashMap<>();
        private long wallNanos;
        private long ioLatencySumNanos;
        private long[] latenciesNanos = new long[0];

        double ioLatencySumSeconds() {
            return ioLatencySumNanos / 1_000_000_000.0;
        }

        double readAmplification() {
            return logicalBytes == 0 ? Double.NaN : (double) remoteBytes / logicalBytes;
        }

        double cacheHitRate() {
            return totalReads == 0 ? Double.NaN : (double) cacheHitReads / totalReads;
        }

        double nsPerOp() {
            return totalReads == 0 ? Double.NaN : (double) wallNanos / totalReads;
        }

        long percentileNanos(int p) {
            if (latenciesNanos.length == 0) {
                return 0L;
            }
            int rank = (int) Math.ceil(p / 100.0 * latenciesNanos.length);
            return latenciesNanos[Math.min(latenciesNanos.length - 1, Math.max(0, rank - 1))];
        }
    }

    @Test
    void benchmark() throws IOException {
        List<Rec> recs = loadTrace();
        assertThat(recs).as("trace records").isNotEmpty();

        cacheBudget = (long) (intProp("s3arr.cacheBudgetMiB", 64)) * 1024 * 1024;
        prefetchBlockSize = (long) intProp("s3arr.prefetchBlockMiB", 1) * 1024 * 1024;
        long requestedMaxFetch = (long) intProp("s3arr.maxFetchMiB", 0) * 1024 * 1024;
        maxFetch = requestedMaxFetch > 0 ? requestedMaxFetch : autoMaxFetch(recs);
        String label = System.getProperty("s3arr.label", "S2");

        // Remote-read cost model (see SimLatency): rtt + bytes/BW is injected as a REAL delay into every GET so that
        // wall-clock / p50-p99 reflect network cost; thinkMs is a synthetic per-read compute gap (all modes equally)
        // that gives async prefetch a window to overlap - without it a back-to-back replay can never hide RTT.
        rttNanos = (long) (doubleProp("s3arr.rttMs", 0.0) * 1_000_000.0);
        bwMiBps = doubleProp("s3arr.bwMiBps", 0.0);
        thinkNanos = (long) (doubleProp("s3arr.thinkMs", 0.0) * 1_000_000.0);
        backend = parseBackend(System.getProperty("s3arr.backend", "synthetic"));
        bucket = "bench";
        if (realBackend()) {
            if (latencyOn()) {
                throw new IllegalArgumentException("s3arr.rttMs and s3arr.bwMiBps are synthetic-only; "
                                                   + "do not combine them with s3arr.backend=s3");
            }
            MinioEnvironment minio = MinioEnvironment.fromEnvironment();
            bucket = minio.bucket();
            realSyncClient = minio.syncClient();
            realAsyncClient = minio.asyncClient();
            preflightObjects(recs);
        }

        // Real sleeps make each pass ~reads*(rtt+think) long; auto-shrink iterations unless the user set them.
        warmup = intProp("s3arr.warmup", latencyOn() || realBackend() ? 0 : 2);
        iters = intProp("s3arr.iters", latencyOn() || realBackend() ? 1 : 5);

        int apps = intProp("s3arr.apps", 1);
        int depth = intProp("s3arr.prefetchDepth", 1);

        if (latencyOn()) {
            ioPool = Executors.newFixedThreadPool(32);
        }
        try {
            String forcePolicy = System.getProperty("s3arr.forcePolicy");
            Map<String, PolicyName> oracleMap = parseOracleMap(System.getProperty("s3arr.oracleMap"));
            String configuredSelector = System.getProperty("s3arr.selector");
            if (forcePolicy != null && oracleMap != null) {
                throw new IllegalArgumentException("Set only one of s3arr.forcePolicy and s3arr.oracleMap");
            }
            if (configuredSelector != null && (forcePolicy != null || oracleMap != null)) {
                throw new IllegalArgumentException("s3arr.selector cannot be combined with forcePolicy or oracleMap");
            }
            if (forcePolicy != null || oracleMap != null || configuredSelector != null) {
                boolean passthroughOnly = "passthrough".equals(configuredSelector);
                SelectorMode mode = passthroughOnly ? SelectorMode.DECISION_TREE : parseSelectorMode(configuredSelector);
                Result configured = passthroughOnly
                                    ? measure("passthrough", recs, false)
                                    : measureS3(forcePolicy == null
                                                ? (oracleMap == null ? configuredSelector : "oracle_perworkload")
                                                : "forced_" + forcePolicy,
                                                recs, apps, depth, mode,
                                                forcePolicy == null ? null : parsePolicy(forcePolicy), oracleMap);
                String report = renderConfigured(label, recs.size(), configured, apps, depth, forcePolicy, oracleMap,
                                                 configuredSelector);
                System.out.println(report);
                writeConfiguredResults(label, report, configured);
                assertThat(configured.totalReads).isEqualTo(recs.size());
                assertThat(configured.fallbackReads).isEqualTo(0L);
                return;
            }
            // Fair budget: template_auto, S2 and S3 all run on the SAME AdaptiveReaderRuntime (same appID organisation,
            // same work-conserving global budget split across the same number of apps) and route among the SAME four
            // executors. They differ only by (a) the policy brain - hand rule vs learned tree - and (b) prefetch depth
            // (S2/template_auto = 0, S3 = depth). passthrough is the raw no-cache baseline. So "S2 vs template_auto"
            // isolates learned-vs-handrule and "S3 vs S2" isolates prefetch, without conflating cache/budget models.
            Result passthrough = measure("passthrough", recs, false);
            Result templateAuto = measureS3("template_auto", recs, apps, 0, SelectorMode.TEMPLATE_AUTO, null, null);
            Result s2 = measureS3("s2", recs, apps, 0, SelectorMode.DECISION_TREE, null, null);
            Result s3 = measureS3("s3", recs, apps, depth, SelectorMode.DECISION_TREE, null, null);

            String report = render(label, recs.size(), passthrough, templateAuto, s2, s3, apps, depth);
            System.out.println(report);
            writeResults(label, report, passthrough, templateAuto, s2, s3);

            // sanity: every mode must complete over every read with no policy fallbacks in the rule/adaptive runs
            assertThat(passthrough.totalReads).isEqualTo(recs.size());
            assertThat(templateAuto.totalReads).isEqualTo(recs.size());
            assertThat(templateAuto.fallbackReads).isEqualTo(0L);
            assertThat(s2.totalReads).isEqualTo(recs.size());
            assertThat(s2.fallbackReads).isEqualTo(0L);
            assertThat(s3.totalReads).isEqualTo(recs.size());
            assertThat(s3.fallbackReads).isEqualTo(0L);
        } finally {
            if (ioPool != null) {
                ioPool.shutdownNow();
                try {
                    ioPool.awaitTermination(10, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (realAsyncClient != null) {
                realAsyncClient.close();
            }
            if (realSyncClient != null) {
                realSyncClient.close();
            }
        }
    }

    private Result measure(String mode, List<Rec> recs, boolean enabled) {
        for (int i = 0; i < warmup; i++) {
            replay(recs, enabled, cacheBudget, maxFetch, false);
        }
        Result best = null;
        for (int i = 0; i < iters; i++) {
            Result r = replay(recs, enabled, cacheBudget, maxFetch, i == 0);
            r.mode = mode;
            if (best == null || r.wallNanos < best.wallNanos) {
                best = r;
            }
        }
        return best;
    }

    private Result replay(List<Rec> recs, boolean enabled, long cacheBudget, long maxFetch, boolean verify) {
        ObjectStore store;
        if (realBackend()) {
            store = new S3ClientObjectStore(realSyncClient, bucket);
        } else {
            GeneratedObjectStore generated = new GeneratedObjectStore().latency(rttNanos, bwMiBps);
            for (Rec r : recs) {
                generated.define(r.key, Math.max(r.fileSize, r.offset + r.length));
            }
            store = generated;
        }
        Map<String, AdaptiveRangeReader> readers = new HashMap<>();
        // Latencies captured every iteration (cheap) so the fastest iteration still reports percentiles.
        long[] lat = new long[recs.size()];
        long ioSum = 0;

        long t0 = System.nanoTime();
        for (int i = 0; i < recs.size(); i++) {
            Rec r = recs.get(i);
            AdaptiveRangeReader reader = readers.computeIfAbsent(r.key, k ->
                enabled ? new AdaptiveRangeReaderImpl(store, k, cacheBudget, maxFetch, null)
                        : new PassthroughRangeReader(store, k));
            byte[] dst = new byte[r.length];
            long s = System.nanoTime();
            int n = reader.read(r.offset, dst, 0, r.length);
            long e = System.nanoTime();
            lat[i] = e - s;
            ioSum += lat[i];
            if (verify) {
                for (int b = 0; b < n; b++) {
                    if (dst[b] != GeneratedObjectStore.byteAt(r.offset + b)) {
                        throw new AssertionError("byte mismatch at " + r.key + "@" + (r.offset + b));
                    }
                }
            }
            SimLatency.sleepNanos(thinkNanos);
        }
        long wall = System.nanoTime() - t0;

        Result res = new Result();
        res.wallNanos = wall;
        res.ioLatencySumNanos = ioSum;
        for (Map.Entry<String, AdaptiveRangeReader> entry : readers.entrySet()) {
            ReaderMetrics m = entry.getValue().metrics();
            accumulate(res, m);
            Result prefix = res.prefixResults.computeIfAbsent(prefixOf(entry.getKey()), ignored -> new Result());
            accumulate(prefix, m);
        }
        Arrays.sort(lat);
        res.latenciesNanos = lat;
        return res;
    }

    private Result measureS3(String mode, List<Rec> recs, int apps, int depth, SelectorMode sel,
                             PolicyName forcedPolicy, Map<String, PolicyName> oracleMap) {
        for (int i = 0; i < warmup; i++) {
            replayS3(recs, apps, depth, sel, forcedPolicy, oracleMap, false);
        }
        Result best = null;
        for (int i = 0; i < iters; i++) {
            Result r = replayS3(recs, apps, depth, sel, forcedPolicy, oracleMap, i == 0);
            r.mode = mode;
            if (best == null || r.wallNanos < best.wallNanos) {
                best = r;
            }
        }
        return best;
    }

    private Result replayS3(List<Rec> recs, int apps, int depth, SelectorMode sel,
                            PolicyName forcedPolicy, Map<String, PolicyName> oracleMap, boolean verify) {
        AsyncObjectStore store = null;
        if (!realBackend()) {
            InMemoryAsyncObjectStore synthetic = new InMemoryAsyncObjectStore();
            if (latencyOn()) {
                // Real delay on a shared pool: demand GETs block the caller (~fetch cost) while prefetch GETs run in
                // the background and can finish during the think-gap, so a later demand finds them cached.
                synthetic.executor(ioPool).latency(rttNanos, bwMiBps);
            }
            for (Rec r : recs) {
                synthetic.define(r.key, Math.max(r.fileSize, r.offset + r.length));
            }
            store = synthetic;
        }
        int nApps = Math.max(1, apps);
        AdaptiveReaderRuntime.Builder runtimeBuilder = AdaptiveReaderRuntime.builder()
                                                                               .globalCacheBytes(cacheBudget)
                                                                               .perAppReservedBytes(cacheBudget / nApps)
                                                                               .prefetchDepth(depth)
                                                                               .prefetchBlockSize(prefetchBlockSize)
                                                                               .selectorMode(sel)
                                                                               .maxConcurrentGets(16)
                                                                               .maxSingleFetchBytes(maxFetch);
        if (realBackend()) {
            runtimeBuilder.s3AsyncClient(realAsyncClient);
        }
        if (forcedPolicy != null) {
            runtimeBuilder.forcePolicy(forcedPolicy);
        } else if (oracleMap != null) {
            runtimeBuilder.prefixPolicyMap(featureKeyPrefixes(oracleMap));
        }
        AdaptiveReaderRuntime runtime = runtimeBuilder.build();
        Result res = new Result();
        try {
            List<AppContext> contexts = new ArrayList<>();
            for (int i = 0; i < nApps; i++) {
                contexts.add(runtime.register("app" + i));
            }
            Map<String, AdaptiveRangeReader> readers = new HashMap<>();
            long[] lat = new long[recs.size()];
            long ioSum = 0;

            long t0 = System.nanoTime();
            for (int i = 0; i < recs.size(); i++) {
                Rec r = recs.get(i);
                AppContext app = contexts.get(Math.floorMod(r.key.hashCode(), nApps));
                final AsyncObjectStore finalStore = store;
                AdaptiveRangeReader reader = readers.computeIfAbsent(r.key, k -> realBackend()
                    ? app.newReader(bucket, k)
                    : app.newReader(finalStore, bucket, k));
                byte[] dst = new byte[r.length];
                long s = System.nanoTime();
                int n = reader.read(r.offset, dst, 0, r.length);
                lat[i] = System.nanoTime() - s;
                ioSum += lat[i];
                if (verify) {
                    for (int b = 0; b < n; b++) {
                        if (dst[b] != GeneratedObjectStore.byteAt(r.offset + b)) {
                            throw new AssertionError("byte mismatch at " + r.key + "@" + (r.offset + b));
                        }
                    }
                }
                SimLatency.sleepNanos(thinkNanos);
            }
            res.wallNanos = System.nanoTime() - t0;
            res.ioLatencySumNanos = ioSum;

            for (Map.Entry<String, AdaptiveRangeReader> entry : readers.entrySet()) {
                AdaptiveRangeReader reader = entry.getValue();
                ReaderMetrics m = reader.metrics();
                accumulate(res, m);
                Result prefix = res.prefixResults.computeIfAbsent(prefixOf(entry.getKey()), ignored -> new Result());
                accumulate(prefix, m);
            }
            res.isolationSummary = isolationSummary(runtime.metrics());
            Arrays.sort(lat);
            res.latenciesNanos = lat;
        } finally {
            runtime.close();
        }
        return res;
    }

    private static String isolationSummary(RuntimeMetrics rm) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("capacity=%.1fMiB totalUsage=%.1fMiB maxConcurrentGets=%d peakGets=%d%n",
                                rm.capacityBytes() / MIB, rm.totalUsageBytes() / MIB, rm.maxConcurrentGets(),
                                rm.concurrentGetPeak()));
        for (Map.Entry<String, AppMetrics> e : rm.perApp().entrySet()) {
            AppMetrics a = e.getValue();
            sb.append(String.format("  %-10s reserved=%.1fMiB usage=%.1fMiB blocks=%d inflightPeak=%.1fMiB%n",
                                    a.appId(), a.reservedBytes() / MIB, a.budgetUsageBytes() / MIB, a.blockCount(),
                                    a.inflightPeakBytes() / MIB));
        }
        return sb.toString();
    }

    private static void accumulate(Result target, ReaderMetrics metrics) {
        target.totalReads += metrics.logicalReads();
        target.logicalBytes += metrics.logicalBytes();
        target.remoteGets += metrics.remoteGets();
        target.remoteBytes += metrics.remoteBytes();
        target.policySwitches += metrics.policySwitches();
        target.fallbackReads += metrics.fallbackReads();
        target.demandClampReads += metrics.demandClampReads();
        target.cacheHitReads += metrics.cacheHitReads();
        target.prefetchGets += metrics.prefetchGets();
        target.prefetchBytes += metrics.prefetchBytes();
        target.prefetchUsefulBytes += metrics.prefetchUsefulBytes();
        target.prefetchWastedBytes += metrics.prefetchWastedBytes();
        target.prefetchCancelledBytes += metrics.prefetchCancelledBytes();
        metrics.policyReadCounts().forEach((k, v) -> target.policyReads.merge(k, v, Long::sum));
    }

    private static String prefixOf(String key) {
        int slash = key.indexOf('/');
        return slash < 0 ? key : key.substring(0, slash);
    }

    private Map<String, PolicyName> featureKeyPrefixes(Map<String, PolicyName> tracePrefixes) {
        Map<String, PolicyName> result = new LinkedHashMap<>();
        for (Map.Entry<String, PolicyName> entry : tracePrefixes.entrySet()) {
            String prefix = entry.getKey();
            String bucketPrefix = bucket + "/";
            result.put(prefix.startsWith(bucketPrefix) ? prefix : bucketPrefix + prefix, entry.getValue());
        }
        return result;
    }

    private String render(String label, int reads, Result pt, Result ta, Result s2, Result s3, int apps, int depth) {
        List<Result> cols = Arrays.asList(pt, ta, s2, s3);
        List<String> heads = Arrays.asList("passthrough(off)", "template_auto(d0)", "S2(learn,d0)",
                                           "S3(learn,d" + depth + ")");

        StringBuilder sb = new StringBuilder();
        sb.append("\n============= S2/S3 Adaptive Range Reader - System Benchmark =============\n");
        sb.append(String.format("label=%s  reads=%d  warmup=%d  iters=%d  apps=%d  globalCache=%dMiB  "
                                + "perAppReserved=%dMiB  maxFetch=%dMiB  prefetchBlock=%dMiB  prefetchDepth=%d%n",
                                label, reads, warmup, iters, apps, cacheBudget / (1024 * 1024),
                                cacheBudget / (1024 * 1024) / Math.max(1, apps), maxFetch / (1024 * 1024),
                                prefetchBlockSize / (1024 * 1024), depth));
        sb.append("(template_auto/S2/S3 share the SAME appID budget/runtime and the same 4 executors;\n");
        sb.append(" they differ only by policy brain (hand rule vs learned tree) and prefetch depth.\n");
        sb.append(" passthrough = raw no-cache baseline.)\n");
        if (latencyOn()) {
            sb.append(String.format(" cost model: rtt=%.2fms  bw=%s  think=%.2fms/read (injected into every GET; "
                                    + "ns/op & IO-latency now reflect network cost)%n%n",
                                    rttNanos / 1_000_000.0,
                                    bwMiBps > 0 ? String.format("%.0fMiB/s", bwMiBps) : "unlimited",
                                    thinkNanos / 1_000_000.0));
        } else {
            sb.append(" cost model: none (zero-RTT synthetic store; ns/op excludes network - IO-shape is primary)\n\n");
        }

        sb.append(String.format("%-22s", "metric"));
        for (String h : heads) {
            sb.append(String.format(" %17s", h));
        }
        sb.append(String.format("%n"));

        sb.append(longRow("logical reads", cols, r -> r.totalReads));
        sb.append(mibRow("logical MiB", cols, r -> r.logicalBytes));
        sb.append(longRow("remote GETs", cols, r -> r.remoteGets));
        sb.append(mibRow("remote MiB", cols, r -> r.remoteBytes));
        sb.append(dblRow("read amplification", cols, Result::readAmplification));
        sb.append(dblRow("cache hit rate", cols, Result::cacheHitRate));
        sb.append(longRow("policy switches", cols, r -> r.policySwitches));
        sb.append(longRow("fallback reads", cols, r -> r.fallbackReads));
        sb.append(longRow("demand clamp reads", cols, r -> r.demandClampReads));
        sb.append(longRow("prefetch GETs", cols, r -> r.prefetchGets));
        sb.append(mibRow("prefetch useful MiB", cols, r -> r.prefetchUsefulBytes));
        sb.append(mibRow("prefetch wasted MiB", cols, r -> r.prefetchWastedBytes));
        sb.append(mibRow("prefetch cancel MiB", cols, r -> r.prefetchCancelledBytes));
        sb.append(dblRow("IO latency sum (s)", cols, Result::ioLatencySumSeconds));
        sb.append(dblRow("ns/op (min iter)", cols, Result::nsPerOp));
        sb.append(longRow("p50 ns", cols, r -> r.percentileNanos(50)));
        sb.append(longRow("p95 ns", cols, r -> r.percentileNanos(95)));
        sb.append(longRow("p99 ns", cols, r -> r.percentileNanos(99)));

        sb.append("\nper-policy read counts (template_auto | learned):\n");
        for (PolicyName p : PolicyName.values()) {
            sb.append(String.format("  %-22s %8d | %8d%n", p.label(), ta.policyReads.getOrDefault(p, 0L),
                                    s3.policyReads.getOrDefault(p, 0L)));
        }

        sb.append("\ndeltas:\n");
        sb.append(deltaLine("  S2(learn) vs template_auto:  ", ta, s2));
        sb.append(deltaLine("  S2 vs passthrough (cache):   ", pt, s2));
        sb.append(deltaLine("  S3 vs S2 (prefetch):         ", s2, s3));
        if (latencyOn()) {
            sb.append("\nIO-latency deltas (lower is better; the point of RTT injection):\n");
            sb.append(latDeltaLine("  S2 vs passthrough (cache):   ", pt, s2));
            sb.append(latDeltaLine("  S3 vs S2 (prefetch):         ", s2, s3));
            sb.append(latDeltaLine("  S3 vs passthrough (total):   ", pt, s3));
        }

        sb.append("\nper-app isolation (S3 RuntimeMetrics):\n");
        sb.append(s3.isolationSummary);
        sb.append("=========================================================================\n");
        return sb.toString();
    }

    private static String deltaLine(String label, Result base, Result other) {
        if (base.remoteGets == 0) {
            return label + "n/a\n";
        }
        return String.format("%sGET %+.1f%%   remote bytes %+.1f%%%n", label,
                             100.0 * (other.remoteGets - base.remoteGets) / base.remoteGets,
                             100.0 * (other.remoteBytes - base.remoteBytes) / Math.max(1, base.remoteBytes));
    }

    private static String latDeltaLine(String label, Result base, Result other) {
        if (base.ioLatencySumNanos == 0) {
            return label + "n/a\n";
        }
        return String.format("%sIO latency %+.1f%%  (%.2fs -> %.2fs)%n", label,
                             100.0 * (other.ioLatencySumNanos - base.ioLatencySumNanos) / base.ioLatencySumNanos,
                             base.ioLatencySumSeconds(), other.ioLatencySumSeconds());
    }

    private static String longRow(String name, List<Result> cols, ToLongFunction<Result> f) {
        StringBuilder sb = new StringBuilder(String.format("%-22s", name));
        for (Result r : cols) {
            sb.append(String.format(" %17d", f.applyAsLong(r)));
        }
        return sb.append(String.format("%n")).toString();
    }

    private static String dblRow(String name, List<Result> cols, ToDoubleFunction<Result> f) {
        StringBuilder sb = new StringBuilder(String.format("%-22s", name));
        for (Result r : cols) {
            sb.append(String.format(" %17.3f", f.applyAsDouble(r)));
        }
        return sb.append(String.format("%n")).toString();
    }

    private static String mibRow(String name, List<Result> cols, ToLongFunction<Result> f) {
        StringBuilder sb = new StringBuilder(String.format("%-22s", name));
        for (Result r : cols) {
            sb.append(String.format(" %17.2f", f.applyAsLong(r) / MIB));
        }
        return sb.append(String.format("%n")).toString();
    }

    private void writeResults(String label, String report, Result pt, Result ta, Result s2, Result s3)
        throws IOException {
        Path dir = Paths.get("target", "s3arr-benchmark");
        Files.createDirectories(dir);
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        Files.write(dir.resolve("report-" + label + "-" + ts + ".txt"), report.getBytes(StandardCharsets.UTF_8));

        Path csv = dir.resolve("results-v2.csv");
        StringBuilder sb = new StringBuilder();
        if (!Files.exists(csv)) {
            sb.append(csvHeader());
        }
        appendResultRows(sb, ts, label, pt);
        appendResultRows(sb, ts, label, ta);
        appendResultRows(sb, ts, label, s2);
        appendResultRows(sb, ts, label, s3);
        Files.write(csv, sb.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        appendLegacyResults(dir.resolve("results.csv"), ts, label, pt, ta, s2, s3);
        System.out.println("[benchmark] wrote " + dir.toAbsolutePath() + " (report-*.txt + results-v2.csv)");
    }

    private void writeConfiguredResults(String label, String report, Result result) throws IOException {
        Path dir = Paths.get("target", "s3arr-benchmark");
        Files.createDirectories(dir);
        String ts = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss"));
        Files.write(dir.resolve("report-" + label + "-" + ts + ".txt"), report.getBytes(StandardCharsets.UTF_8));

        Path csv = dir.resolve("results-v2.csv");
        StringBuilder sb = new StringBuilder();
        if (!Files.exists(csv)) {
            sb.append(csvHeader());
        }
        appendResultRows(sb, ts, label, result);
        Files.write(csv, sb.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        System.out.println("[benchmark] wrote " + dir.toAbsolutePath() + " (report-*.txt + results-v2.csv)");
    }

    private static String csvHeader() {
        return "timestamp,label,mode,scope,prefix,reads,logicalBytes,remoteGets,remoteBytes,readAmp,cacheHitRate,"
               + "policySwitches,fallbackReads,demandClampReads,ioLatencySumNanos,nsPerOp,p50Ns,p95Ns,p99Ns,"
               + "prefetchGets,prefetchBytes,prefetchUsefulBytes,prefetchWastedBytes,prefetchCancelledBytes\n";
    }

    private static void appendLegacyResults(Path csv, String ts, String label, Result... results) throws IOException {
        StringBuilder sb = new StringBuilder();
        if (!Files.exists(csv)) {
            sb.append("timestamp,label,mode,reads,logicalBytes,remoteGets,remoteBytes,readAmp,cacheHitRate,"
                      + "policySwitches,fallbackReads,ioLatencySumNanos,nsPerOp,p50Ns,p95Ns,p99Ns,"
                      + "prefetchGets,prefetchBytes,prefetchUsefulBytes,prefetchWastedBytes,prefetchCancelledBytes\n");
        }
        for (Result result : results) {
            sb.append(String.format("%s,%s,%s,%d,%d,%d,%d,%.4f,%.4f,%d,%d,%d,%.1f,%d,%d,%d,%d,%d,%d,%d,%d%n",
                                    ts, label, result.mode, result.totalReads, result.logicalBytes, result.remoteGets,
                                    result.remoteBytes, result.readAmplification(), result.cacheHitRate(),
                                    result.policySwitches, result.fallbackReads, result.ioLatencySumNanos,
                                    result.nsPerOp(), result.percentileNanos(50), result.percentileNanos(95),
                                    result.percentileNanos(99), result.prefetchGets, result.prefetchBytes,
                                    result.prefetchUsefulBytes, result.prefetchWastedBytes,
                                    result.prefetchCancelledBytes));
        }
        Files.write(csv, sb.toString().getBytes(StandardCharsets.UTF_8),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static void appendResultRows(StringBuilder sb, String ts, String label, Result result) {
        sb.append(csvRow(ts, label, result, "all", ""));
        for (Map.Entry<String, Result> entry : result.prefixResults.entrySet()) {
            Result prefix = entry.getValue();
            prefix.mode = result.mode;
            sb.append(csvRow(ts, label, prefix, "prefix", entry.getKey()));
        }
    }

    private static String csvRow(String ts, String label, Result r, String scope, String prefix) {
        return String.format("%s,%s,%s,%s,%s,%d,%d,%d,%d,%.4f,%.4f,%d,%d,%d,%d,%.1f,%d,%d,%d,%d,%d,%d,%d,%d%n",
                             ts, label, r.mode, scope, prefix, r.totalReads, r.logicalBytes, r.remoteGets,
                             r.remoteBytes, r.readAmplification(), r.cacheHitRate(), r.policySwitches,
                             r.fallbackReads, r.demandClampReads, r.ioLatencySumNanos, r.nsPerOp(),
                             r.percentileNanos(50), r.percentileNanos(95), r.percentileNanos(99), r.prefetchGets,
                             r.prefetchBytes, r.prefetchUsefulBytes, r.prefetchWastedBytes, r.prefetchCancelledBytes);
    }

    private String renderConfigured(String label, int reads, Result result, int apps, int depth, String forcePolicy,
                                    Map<String, PolicyName> oracleMap, String configuredSelector) {
        String selector = forcePolicy != null ? "fixed:" + forcePolicy
                          : oracleMap != null ? "prefix oracle:" + oracleMap : configuredSelector;
        StringBuilder sb = new StringBuilder();
        sb.append("\n============= Adaptive Range Reader - Policy Sweep =============\n");
        sb.append(String.format("label=%s selector=%s reads=%d warmup=%d iters=%d apps=%d globalCache=%dMiB "
                                + "maxFetch=%dMiB prefetchBlock=%dMiB prefetchDepth=%d%n",
                                label, selector, reads, warmup, iters, apps, cacheBudget / (1024 * 1024),
                                maxFetch / (1024 * 1024), prefetchBlockSize / (1024 * 1024), depth));
        sb.append(String.format("remote GETs=%d remote MiB=%.2f readAmp=%.4f cacheHitRate=%.4f "
                                + "demandClampReads=%d%n",
                                result.remoteGets, result.remoteBytes / MIB, result.readAmplification(),
                                result.cacheHitRate(), result.demandClampReads));
        sb.append("per-prefix metrics:\n");
        for (Map.Entry<String, Result> entry : result.prefixResults.entrySet()) {
            Result prefix = entry.getValue();
            sb.append(String.format("  %-16s reads=%5d remoteMiB=%9.2f amp=%.4f clamps=%d%n", entry.getKey(),
                                    prefix.totalReads, prefix.remoteBytes / MIB, prefix.readAmplification(),
                                    prefix.demandClampReads));
        }
        sb.append("=================================================================\n");
        return sb.toString();
    }

    private long autoMaxFetch(List<Rec> recs) {
        long maxRead = 0L;
        for (Rec rec : recs) {
            maxRead = Math.max(maxRead, rec.length);
        }
        return Math.max(8L * 1024 * 1024, 2L * prefetchBlockSize + maxRead);
    }

    private static PolicyName parsePolicy(String label) {
        PolicyName policy = PolicyName.fromLabel(label.trim());
        if (policy == null) {
            throw new IllegalArgumentException("Unknown policy label for s3arr.forcePolicy: " + label);
        }
        return policy;
    }

    private static Backend parseBackend(String value) {
        if ("synthetic".equals(value)) {
            return Backend.SYNTHETIC;
        }
        if ("s3".equals(value)) {
            return Backend.S3;
        }
        throw new IllegalArgumentException("Unknown s3arr.backend: " + value + " (expected synthetic or s3)");
    }

    private static SelectorMode parseSelectorMode(String selector) {
        if (selector == null || selector.trim().isEmpty() || selector.equals("decision_tree")) {
            return SelectorMode.DECISION_TREE;
        }
        if (selector.equals("template_auto")) {
            return SelectorMode.TEMPLATE_AUTO;
        }
        throw new IllegalArgumentException("Unknown s3arr.selector: " + selector);
    }

    private static Map<String, PolicyName> parseOracleMap(String encoded) {
        if (encoded == null || encoded.trim().isEmpty()) {
            return null;
        }
        Map<String, PolicyName> result = new LinkedHashMap<>();
        for (String assignment : encoded.split(",")) {
            String[] pair = assignment.trim().split(":", 2);
            if (pair.length != 2 || pair[0].trim().isEmpty()) {
                throw new IllegalArgumentException("Invalid s3arr.oracleMap entry: " + assignment);
            }
            result.put(pair[0].trim(), parsePolicy(pair[1]));
        }
        return result;
    }

    private static int intProp(String key, int dflt) {
        String v = System.getProperty(key);
        return v == null ? dflt : Integer.parseInt(v.trim());
    }

    private static double doubleProp(String key, double dflt) {
        String v = System.getProperty(key);
        return v == null ? dflt : Double.parseDouble(v.trim());
    }

    private void preflightObjects(List<Rec> recs) {
        Map<String, Long> requiredSizes = new LinkedHashMap<>();
        for (Rec rec : recs) {
            requiredSizes.merge(rec.key, Math.max(rec.fileSize, rec.offset + rec.length), Math::max);
        }
        List<String> invalid = new ArrayList<>();
        for (Map.Entry<String, Long> entry : requiredSizes.entrySet()) {
            try {
                HeadObjectResponse head = realSyncClient.headObject(builder -> builder.bucket(bucket).key(entry.getKey()));
                Long actual = head.contentLength();
                if (actual == null || actual < entry.getValue()) {
                    invalid.add(entry.getKey() + " requires " + entry.getValue() + " bytes but has " + actual);
                }
            } catch (RuntimeException e) {
                invalid.add(entry.getKey() + " is unavailable: " + e.getMessage());
            }
        }
        if (!invalid.isEmpty()) {
            throw new IllegalStateException("MinIO preflight failed for bucket " + bucket + ":\n  "
                                            + String.join("\n  ", invalid)
                                            + "\nRun MinioTraceObjectProvisioner first.");
        }
        System.out.println("[benchmark] MinIO preflight validated " + requiredSizes.size() + " objects in bucket "
                           + bucket);
    }

    private List<Rec> loadTrace() throws IOException {
        String override = System.getProperty("s3arr.trace");
        if (override != null) {
            return parse(Files.newInputStream(Paths.get(override)), override);
        }
        Path repoTrace = findUpwards("traces/tpch_sf1_full.csv");
        if (repoTrace != null) {
            return parse(Files.newInputStream(repoTrace), repoTrace.toString());
        }
        InputStream res = getClass().getResourceAsStream("/traces/tpch_slice.csv");
        if (res == null) {
            throw new IOException("no trace found: set -Ds3arr.trace=/path/to/trace.csv");
        }
        return parse(res, "classpath:/traces/tpch_slice.csv");
    }

    private static Path findUpwards(String relative) {
        Path dir = Paths.get("").toAbsolutePath();
        for (int i = 0; i < 8 && dir != null; i++) {
            Path candidate = dir.resolve(relative);
            if (Files.exists(candidate)) {
                return candidate;
            }
            dir = dir.getParent();
        }
        return null;
    }

    private static List<Rec> parse(InputStream in, String source) throws IOException {
        List<Rec> recs = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line = reader.readLine(); // header
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] f = line.split(",");
                recs.add(new Rec(f[1], Long.parseLong(f[2]), Integer.parseInt(f[3]), Long.parseLong(f[4])));
            }
        }
        System.out.println("[benchmark] loaded " + recs.size() + " records from " + source);
        return recs;
    }
}
