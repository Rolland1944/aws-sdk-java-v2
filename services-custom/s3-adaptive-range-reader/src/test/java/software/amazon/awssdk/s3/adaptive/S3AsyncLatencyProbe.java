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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.HttpStatusCode;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Scout-round E1 remake: bare S3 latency / bandwidth / concurrency with a <em>single reused</em>
 * {@link S3AsyncClient}. Intentionally not named {@code *Test} so Surefire does not pick it up.
 *
 * <p>The boto3 probe created a new client per GET and therefore measured TLS handshake, not
 * saturated S3. This probe:
 * <ul>
 *   <li>builds one Netty client with {@code maxConcurrency >= 128} and reuses it;</li>
 *   <li>warms connections before timing;</li>
 *   <li>spreads GETs across many keys under {@code tpch300/lineitem/} so a single partition
 *       limiter cannot masquerade as bandwidth saturation;</li>
 *   <li>counts HTTP 503 / SDK retries via an interceptor.</li>
 * </ul>
 *
 * <pre>
 * mvn -q -pl services-custom/s3-adaptive-range-reader \
 *     -Djapicmp.skip=true -Dcheckstyle.skip=true -Dspotbugs.skip=true \
 *     -Dtest=S3AsyncLatencyProbe test
 * </pre>
 */
class S3AsyncLatencyProbe {

    private static final long[] SIZE_SWEEP = {4L * 1024, 64L * 1024, 1024L * 1024, 16L * 1024 * 1024};
    private static final long[] CONC_SIZES = {64L * 1024, 1024L * 1024, 8L * 1024 * 1024};
    private static final int[] CONCURRENCIES = {1, 4, 16, 64};

    private final AtomicInteger http503 = new AtomicInteger();
    private final AtomicInteger retries = new AtomicInteger();
    private final AtomicInteger otherErrors = new AtomicInteger();

    @Test
    void probe() throws IOException {
        MinioEnvironment env = MinioEnvironment.fromEnvironment();
        Path outDir = Paths.get(System.getProperty(
            "s3arr.probe.outDir",
            "docs/adaptive-range-reader/results/aws-s3_scout_e1_java"));
        Files.createDirectories(outDir);

        int nPerSize = intProp("s3arr.probe.nPerSize", 300);
        int warmupGets = intProp("s3arr.probe.warmupGets", 80);
        int samplesPerWorker = intProp("s3arr.probe.samplesPerWorker", 20);
        int maxKeys = intProp("s3arr.probe.maxKeys", 32);
        String prefix = System.getProperty("s3arr.probe.prefix", "tpch300/lineitem/");
        int maxConc = intProp("s3arr.probe.maxConcurrency", 128);

        RetryCounterInterceptor counter = new RetryCounterInterceptor(http503, retries);
        try (S3AsyncClient s3 = S3AsyncClient.builder()
                                             .endpointOverride(env.endpoint())
                                             .region(env.region())
                                             .credentialsProvider(env.credentials())
                                             .serviceConfiguration(MinioEnvironment.s3ConfigurationPublic())
                                             .httpClient(NettyNioAsyncHttpClient.builder()
                                                                                .maxConcurrency(maxConc)
                                                                                .build())
                                             .overrideConfiguration(ClientOverrideConfiguration.builder()
                                                                                               .addExecutionInterceptor(counter)
                                                                                               .build())
                                             .build()) {
            List<String> keys = listKeys(s3, env.bucket(), prefix, maxKeys);
            if (keys.isEmpty()) {
                throw new IllegalStateException("no objects under s3://" + env.bucket() + "/" + prefix);
            }
            String primary = keys.get(0);
            long objectSize = s3.headObject(HeadObjectRequest.builder()
                                                             .bucket(env.bucket())
                                                             .key(primary)
                                                             .build())
                                .join()
                                .contentLength();
            System.out.println("[probe] client=S3AsyncClient maxConcurrency=" + maxConc
                               + " bucket=" + env.bucket() + " keys=" + keys.size()
                               + " primary=" + primary + " size=" + objectSize);

            warmup(s3, env.bucket(), keys, warmupGets, 1024 * 1024);
            System.out.println("[probe] warmup done");

            List<SizeRow> sizeRows = new ArrayList<SizeRow>();
            for (int i = 0; i < SIZE_SWEEP.length; i++) {
                long size = SIZE_SWEEP[i];
                sizeRows.add(sizeSweep(s3, env.bucket(), primary, size, nPerSize));
            }

            RttBwFit fit = fitRttBw(sizeRows);
            System.out.println("[probe] fit rttMs=" + String.format(Locale.US, "%.2f", fit.rttS * 1e3)
                               + " bwMiBps=" + String.format(Locale.US, "%.1f", fit.bwMibps)
                               + " gStarMiB=" + String.format(Locale.US, "%.2f", fit.gStarMib));

            List<ConcRow> concRows = new ArrayList<ConcRow>();
            for (int ci = 0; ci < CONCURRENCIES.length; ci++) {
                int c = CONCURRENCIES[ci];
                for (int si = 0; si < CONC_SIZES.length; si++) {
                    long size = CONC_SIZES[si];
                    int perWorker = size >= 8L * 1024 * 1024 ? Math.max(4, samplesPerWorker / 4) : samplesPerWorker;
                    concRows.add(concurrencySweep(s3, env.bucket(), keys, c, size, perWorker));
                }
            }

            writeOutputs(outDir, env.bucket(), primary, objectSize, keys.size(), maxConc,
                         sizeRows, fit, concRows);
            System.out.println("[probe] wrote " + outDir.toAbsolutePath());
        }
    }

    private void warmup(S3AsyncClient s3, String bucket, List<String> keys, int n, long size) {
        List<CompletableFuture<Long>> futs = new ArrayList<CompletableFuture<Long>>(n);
        for (int i = 0; i < n; i++) {
            futs.add(timedGet(s3, bucket, keys.get(i % keys.size()), (i * size) % (64L * 1024 * 1024), size));
        }
        CompletableFuture.allOf(futs.toArray(new CompletableFuture[0])).join();
    }

    private SizeRow sizeSweep(S3AsyncClient s3, String bucket, String key, long size, int n) {
        List<Double> lats = new ArrayList<Double>(n);
        for (int i = 0; i < n; i++) {
            long off = (i * Math.max(size, 4096L)) % (64L * 1024 * 1024);
            lats.add(timedGet(s3, bucket, key, off, size).join() / 1e9);
        }
        SizeRow row = summarizeSize(size, lats);
        System.out.println(String.format(Locale.US,
            "[size] %10d B  n=%d  p50=%.2fms  p99=%.2fms  p99/p50=%.2f",
            size, n, row.p50S * 1e3, row.p99S * 1e3, row.p99OverP50));
        return row;
    }

    private ConcRow concurrencySweep(S3AsyncClient s3, String bucket, List<String> keys,
                                     int concurrency, long size, int perWorker) {
        int n = concurrency * perWorker;
        List<CompletableFuture<Long>> futs = new ArrayList<CompletableFuture<Long>>(n);
        long t0 = System.nanoTime();
        for (int i = 0; i < n; i++) {
            String key = keys.get(i % keys.size());
            long off = (i * size) % (32L * 1024 * 1024);
            futs.add(timedGet(s3, bucket, key, off, size));
        }
        CompletableFuture.allOf(futs.toArray(new CompletableFuture[0])).join();
        double wallS = (System.nanoTime() - t0) / 1e9;
        List<Double> lats = new ArrayList<Double>(n);
        for (int i = 0; i < futs.size(); i++) {
            lats.add(futs.get(i).join() / 1e9);
        }
        ConcRow row = summarizeConc(concurrency, size, n, wallS, lats);
        System.out.println(String.format(Locale.US,
            "[conc] c=%-3d size=%d  wall=%.1fs  agg=%.1f MiB/s  p50=%.2fms  p99=%.2fms  p99/p50=%.2f  err503=%d retries=%d",
            concurrency, size, wallS, row.aggMibps, row.p50S * 1e3, row.p99S * 1e3, row.p99OverP50,
            http503.get(), retries.get()));
        return row;
    }

    private CompletableFuture<Long> timedGet(final S3AsyncClient s3, final String bucket, final String key,
                                             final long offset, final long length) {
        final long t0 = System.nanoTime();
        GetObjectRequest req = GetObjectRequest.builder()
                                               .bucket(bucket)
                                               .key(key)
                                               .range("bytes=" + offset + "-" + (offset + length - 1))
                                               .build();
        return s3.getObject(req, AsyncResponseTransformer.toBytes())
                 .handle((resp, err) -> {
                         if (err != null) {
                             Throwable cause = unwrap(err);
                             if (cause instanceof S3Exception) {
                                 int code = ((S3Exception) cause).statusCode();
                                 if (code == 503) {
                                     http503.incrementAndGet();
                                 } else {
                                     otherErrors.incrementAndGet();
                                 }
                             } else {
                                 otherErrors.incrementAndGet();
                             }
                             throw new CompletionException(cause);
                         }
                         int got = resp.asByteArrayUnsafe().length;
                         if (got != length) {
                             throw new CompletionException(new IllegalStateException(
                                 "short read " + key + " got=" + got + " want=" + length));
                         }
                         return System.nanoTime() - t0;
                     });
    }

    private static Throwable unwrap(Throwable err) {
        Throwable cause = err;
        while (cause instanceof CompletionException && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    private List<String> listKeys(S3AsyncClient s3, String bucket, String prefix, int maxKeys) {
        List<String> keys = new ArrayList<String>();
        String token = null;
        do {
            ListObjectsV2Request.Builder b = ListObjectsV2Request.builder()
                                                                 .bucket(bucket)
                                                                 .prefix(prefix)
                                                                 .maxKeys(Math.min(1000, maxKeys));
            if (token != null) {
                b.continuationToken(token);
            }
            ListObjectsV2Response resp = s3.listObjectsV2(b.build()).join();
            for (S3Object obj : resp.contents()) {
                if (obj.size() != null && obj.size() >= 16L * 1024 * 1024) {
                    keys.add(obj.key());
                    if (keys.size() >= maxKeys) {
                        return keys;
                    }
                }
            }
            token = Boolean.TRUE.equals(resp.isTruncated()) ? resp.nextContinuationToken() : null;
        } while (token != null);
        return keys;
    }

    private static SizeRow summarizeSize(long size, List<Double> lats) {
        SizeRow r = new SizeRow();
        r.size = size;
        r.n = lats.size();
        r.p50S = percentile(lats, 50);
        r.p95S = percentile(lats, 95);
        r.p99S = percentile(lats, 99);
        r.p999S = percentile(lats, 99.9);
        r.meanS = mean(lats);
        r.p99OverP50 = r.p50S > 0 ? r.p99S / r.p50S : Double.NaN;
        return r;
    }

    private static ConcRow summarizeConc(int c, long size, int n, double wallS, List<Double> lats) {
        ConcRow r = new ConcRow();
        r.concurrency = c;
        r.size = size;
        r.n = n;
        r.wallS = wallS;
        r.aggMibps = (n * size / wallS) / (1024.0 * 1024.0);
        r.p50S = percentile(lats, 50);
        r.p95S = percentile(lats, 95);
        r.p99S = percentile(lats, 99);
        r.p99OverP50 = r.p50S > 0 ? r.p99S / r.p50S : Double.NaN;
        return r;
    }

    private static RttBwFit fitRttBw(List<SizeRow> rows) {
        SizeRow small = null;
        SizeRow large = null;
        for (int i = 0; i < rows.size(); i++) {
            SizeRow r = rows.get(i);
            if (r.size >= 64 * 1024 && (small == null || r.size < small.size)) {
                small = r;
            }
            if (large == null || r.size > large.size) {
                large = r;
            }
        }
        RttBwFit fit = new RttBwFit();
        if (small == null || large == null || small.size == large.size || large.p50S <= small.p50S) {
            fit.note = "degenerate";
            return fit;
        }
        fit.bwBps = (large.size - small.size) / (large.p50S - small.p50S);
        fit.rttS = small.p50S - small.size / fit.bwBps;
        if (fit.rttS < 0) {
            fit.rttS = Math.min(small.p50S, large.p50S) * 0.5;
        }
        fit.bwMibps = fit.bwBps / (1024.0 * 1024.0);
        fit.gStarB = fit.rttS * fit.bwBps;
        fit.gStarMib = fit.gStarB / (1024.0 * 1024.0);
        return fit;
    }

    private static double percentile(List<Double> xs, double p) {
        if (xs.isEmpty()) {
            return 0;
        }
        List<Double> ys = new ArrayList<Double>(xs);
        Collections.sort(ys);
        int rank = (int) Math.ceil(p / 100.0 * ys.size());
        int idx = Math.min(ys.size() - 1, Math.max(0, rank - 1));
        return ys.get(idx);
    }

    private static double mean(List<Double> xs) {
        double s = 0;
        for (int i = 0; i < xs.size(); i++) {
            s += xs.get(i);
        }
        return xs.isEmpty() ? 0 : s / xs.size();
    }

    private void writeOutputs(Path outDir, String bucket, String primary, long objectSize, int nKeys,
                              int maxConc, List<SizeRow> sizeRows, RttBwFit fit, List<ConcRow> concRows)
        throws IOException {
        StringBuilder json = new StringBuilder();
        json.append("{\n");
        json.append("  \"client\": \"S3AsyncClient+Netty\",\n");
        json.append("  \"bucket\": \"").append(esc(bucket)).append("\",\n");
        json.append("  \"key\": \"").append(esc(primary)).append("\",\n");
        json.append("  \"nKeys\": ").append(nKeys).append(",\n");
        json.append("  \"object_size\": ").append(objectSize).append(",\n");
        json.append("  \"maxConcurrency\": ").append(maxConc).append(",\n");
        json.append("  \"http503\": ").append(http503.get()).append(",\n");
        json.append("  \"retries\": ").append(retries.get()).append(",\n");
        json.append("  \"otherErrors\": ").append(otherErrors.get()).append(",\n");
        json.append("  \"size_summary\": [\n");
        for (int i = 0; i < sizeRows.size(); i++) {
            SizeRow r = sizeRows.get(i);
            json.append("    {\"size\": ").append(r.size)
                .append(", \"n\": ").append(r.n)
                .append(", \"p50_s\": ").append(num(r.p50S))
                .append(", \"p95_s\": ").append(num(r.p95S))
                .append(", \"p99_s\": ").append(num(r.p99S))
                .append(", \"p999_s\": ").append(num(r.p999S))
                .append(", \"mean_s\": ").append(num(r.meanS))
                .append(", \"p99_over_p50\": ").append(num(r.p99OverP50))
                .append("}");
            json.append(i + 1 < sizeRows.size() ? ",\n" : "\n");
        }
        json.append("  ],\n");
        json.append("  \"rtt_bw_fit\": {")
            .append("\"rtt_s\": ").append(num(fit.rttS))
            .append(", \"bw_Bps\": ").append(num(fit.bwBps))
            .append(", \"bw_MiBps\": ").append(num(fit.bwMibps))
            .append(", \"g_star_B\": ").append(num(fit.gStarB))
            .append(", \"g_star_MiB\": ").append(num(fit.gStarMib));
        if (fit.note != null) {
            json.append(", \"note\": \"").append(esc(fit.note)).append("\"");
        }
        json.append("},\n");
        json.append("  \"concurrency_summary\": [\n");
        for (int i = 0; i < concRows.size(); i++) {
            ConcRow r = concRows.get(i);
            json.append("    {\"concurrency\": ").append(r.concurrency)
                .append(", \"size\": ").append(r.size)
                .append(", \"n\": ").append(r.n)
                .append(", \"wall_s\": ").append(num(r.wallS))
                .append(", \"agg_MiBps\": ").append(num(r.aggMibps))
                .append(", \"p50_s\": ").append(num(r.p50S))
                .append(", \"p95_s\": ").append(num(r.p95S))
                .append(", \"p99_s\": ").append(num(r.p99S))
                .append(", \"p99_over_p50\": ").append(num(r.p99OverP50))
                .append("}");
            json.append(i + 1 < concRows.size() ? ",\n" : "\n");
        }
        json.append("  ],\n");
        Double p99p50 = null;
        for (int i = 0; i < sizeRows.size(); i++) {
            if (sizeRows.get(i).size == 1024 * 1024) {
                p99p50 = sizeRows.get(i).p99OverP50;
            }
        }
        json.append("  \"d5_gate\": {\"p99_over_p50_1MiB\": ").append(num(p99p50))
            .append(", \"keep_d5_if_gt_3\": ").append(p99p50 != null && p99p50 > 3.0)
            .append("}\n");
        json.append("}\n");
        Files.write(outDir.resolve("e1_java_summary.json"), json.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static String num(Double v) {
        if (v == null || v.isNaN()) {
            return "null";
        }
        return String.format(Locale.US, "%.9f", v);
    }

    private static String num(double v) {
        if (Double.isNaN(v)) {
            return "null";
        }
        return String.format(Locale.US, "%.9f", v);
    }

    private static String esc(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static int intProp(String key, int dflt) {
        String v = System.getProperty(key);
        return v == null ? dflt : Integer.parseInt(v.trim());
    }

    private static final class SizeRow {
        long size;
        int n;
        double p50S;
        double p95S;
        double p99S;
        double p999S;
        double meanS;
        double p99OverP50;
    }

    private static final class ConcRow {
        int concurrency;
        long size;
        int n;
        double wallS;
        double aggMibps;
        double p50S;
        double p95S;
        double p99S;
        double p99OverP50;
    }

    private static final class RttBwFit {
        double rttS;
        double bwBps;
        double bwMibps;
        double gStarB;
        double gStarMib;
        String note;
    }

    /**
     * Counts HTTP 503 responses. Retry counting is not done via ThreadLocal (async Netty
     * transmissions do not stay on the calling thread).
     */
    private static final class RetryCounterInterceptor implements ExecutionInterceptor {
        private final AtomicInteger http503;
        private final AtomicInteger retries;

        RetryCounterInterceptor(AtomicInteger http503, AtomicInteger retries) {
            this.http503 = http503;
            this.retries = retries;
        }

        @Override
        public void afterTransmission(Context.AfterTransmission context, ExecutionAttributes executionAttributes) {
            SdkHttpResponse resp = context.httpResponse();
            if (resp != null && resp.statusCode() == HttpStatusCode.SERVICE_UNAVAILABLE) {
                http503.incrementAndGet();
                retries.incrementAndGet();
            }
        }
    }
}
