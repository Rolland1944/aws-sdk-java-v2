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

package software.amazon.awssdk.s3.adaptive.telemetry;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;

/**
 * {@code SdkIoCollector} for Track 2: records one NDJSON line per S3 HTTP attempt so that physical range reads can be
 * correlated back to query semantics and Parquet metadata.
 *
 * <p>See {@code docs/adaptive-range-reader/TRACK2_M0_CONTRACT.md} 3.1/3.2 for the record contract and 1.4 for how it is
 * wired in. Installation is entirely non-invasive: add this class name to the S3A option
 * {@code fs.s3a.audit.execution.interceptors}, which requires Hadoop 3.4.0+ (earlier releases run S3A on AWS SDK v1 and
 * ignore the option with only a warning).
 *
 * <p>Three deliberate design constraints:
 *
 * <ol>
 * <li><b>No Hadoop dependency.</b> S3A only requires an {@link ExecutionInterceptor}; implementing
 *     {@code org.apache.hadoop.conf.Configurable} is optional. Configuration is therefore read from
 *     {@link Track2CollectorSetting}, which keeps this module free of any Hadoop version coupling and lets the same
 *     jar work on any S3A release.</li>
 * <li><b>Headers are read in {@link #beforeTransmission}, not while modifying the request.</b> Every interceptor's
 *     {@code modifyHttpRequest} in the chain runs before any {@code beforeTransmission}, so the audit
 *     {@code Referer} header added by S3A's own logging auditor is guaranteed to be visible here regardless of where
 *     this interceptor sits in the chain.</li>
 * <li><b>Records are serialised by hand.</b> This code runs inside the engine's classpath, where competing Jackson
 *     versions are a well known source of breakage; emitting NDJSON directly avoids depending on any JSON library.</li>
 * </ol>
 *
 * <p><b>What {@code latency_ns} means.</b> An interceptor observes the response when its headers arrive, not when the
 * body has been consumed; for streamed {@code GetObject} responses the body is read later by the caller. The recorded
 * latency is therefore <em>time to first byte</em>, not full transfer time. This is the RTT-dominated component the
 * cost model in {@code PROJECT3.md} 3.4 cares about, but it must not be reported as end-to-end transfer time.
 *
 * <p>Every callback swallows its own failures. An interceptor that throws would fail the underlying S3 request, so
 * losing a telemetry record is always preferable to breaking the read being measured.
 */
@SdkInternalApi
public final class Track2IoCollectorInterceptor implements ExecutionInterceptor {

    private static final ExecutionAttribute<Long> START_NANOS = new ExecutionAttribute<>("track2.startNanos");
    private static final ExecutionAttribute<Long> START_MILLIS = new ExecutionAttribute<>("track2.startMillis");
    private static final ExecutionAttribute<Integer> INFLIGHT_AT_ISSUE = new ExecutionAttribute<>("track2.inflight");
    private static final ExecutionAttribute<AtomicInteger> ATTEMPT = new ExecutionAttribute<>("track2.attempt");
    private static final ExecutionAttribute<AtomicInteger> OUTSTANDING = new ExecutionAttribute<>("track2.outstanding");

    /** Concurrent in-flight transmissions across the whole JVM, sampled when each request is issued. */
    private static final AtomicInteger INFLIGHT = new AtomicInteger();

    private final boolean enabled;
    private final List<String> correlationKeys;
    private final RecordSink sink;

    public Track2IoCollectorInterceptor() {
        this.enabled = Track2CollectorSetting.ENABLED.getBooleanValue().orElse(true);
        this.correlationKeys = Arrays.asList(
            Track2CollectorSetting.CORRELATION_KEYS.getStringValueOrThrow().split("\\s*,\\s*"));
        this.sink = enabled ? RecordSink.shared() : null;
    }

    Track2IoCollectorInterceptor(RecordSink sink, List<String> correlationKeys) {
        this.enabled = true;
        this.correlationKeys = correlationKeys;
        this.sink = sink;
    }

    static int inflight() {
        return INFLIGHT.get();
    }

    @Override
    public void beforeExecution(Context.BeforeExecution context, ExecutionAttributes executionAttributes) {
        if (!enabled) {
            return;
        }
        executionAttributes.putAttribute(ATTEMPT, new AtomicInteger());
        executionAttributes.putAttribute(OUTSTANDING, new AtomicInteger());
    }

    @Override
    public void beforeTransmission(Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
        if (!enabled) {
            return;
        }
        try {
            executionAttributes.putAttribute(START_NANOS, System.nanoTime());
            executionAttributes.putAttribute(START_MILLIS, System.currentTimeMillis());
            AtomicInteger outstanding = executionAttributes.getAttribute(OUTSTANDING);
            if (outstanding != null) {
                outstanding.incrementAndGet();
            }
            executionAttributes.putAttribute(INFLIGHT_AT_ISSUE, INFLIGHT.incrementAndGet());
        } catch (RuntimeException e) {
            // never break the read path we are trying to measure
        }
    }

    @Override
    public void afterTransmission(Context.AfterTransmission context, ExecutionAttributes executionAttributes) {
        if (!enabled) {
            return;
        }
        try {
            releaseInflight(executionAttributes);
            emit(context.httpRequest(), context.httpResponse(), executionAttributes, null);
        } catch (Throwable t) {
            // dropping a telemetry record is always preferable to failing the request
        }
    }

    @Override
    public void onExecutionFailure(Context.FailedExecution context, ExecutionAttributes executionAttributes) {
        if (!enabled) {
            return;
        }
        try {
            releaseInflight(executionAttributes);
            emit(context.httpRequest().orElse(null),
                 context.httpResponse().orElse(null),
                 executionAttributes,
                 context.exception());
        } catch (Throwable t) {
            // as above
        }
    }

    /**
     * Balances the in-flight counter for this execution.
     *
     * <p>A failed attempt never reaches {@link #afterTransmission}, so the per-execution outstanding count is drained
     * here as well; without this the JVM-wide gauge would drift upwards on every retry and quietly become meaningless.
     */
    private void releaseInflight(ExecutionAttributes executionAttributes) {
        AtomicInteger outstanding = executionAttributes.getAttribute(OUTSTANDING);
        if (outstanding == null) {
            return;
        }
        while (true) {
            int current = outstanding.get();
            if (current <= 0) {
                return;
            }
            if (outstanding.compareAndSet(current, current - 1)) {
                INFLIGHT.decrementAndGet();
                return;
            }
        }
    }

    private void emit(SdkHttpRequest httpRequest,
                      SdkHttpResponse httpResponse,
                      ExecutionAttributes attributes,
                      Throwable failure) {
        long endNanos = System.nanoTime();
        Long startNanos = attributes.getAttribute(START_NANOS);
        Long startMillis = attributes.getAttribute(START_MILLIS);
        AtomicInteger attempt = attributes.getAttribute(ATTEMPT);

        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("ts_wall_ms", startMillis != null ? startMillis : Long.valueOf(System.currentTimeMillis()));
        rec.put("ts_start_ns", startNanos);
        rec.put("ts_end_ns", endNanos);
        rec.put("latency_ns", startNanos == null ? null : endNanos - startNanos);
        rec.put("attempt", attempt == null ? 0 : attempt.getAndIncrement());
        rec.put("inflight_at_issue", attributes.getAttribute(INFLIGHT_AT_ISSUE));
        rec.put("thread", Thread.currentThread().getName());

        if (httpRequest != null) {
            rec.put("method", httpRequest.method() == null ? null : httpRequest.method().name());
            rec.put("host", httpRequest.host());
            rec.put("path", httpRequest.encodedPath());
            String range = header(httpRequest, "Range");
            rec.put("range_header", range);
            long[] parsed = parseRange(range);
            rec.put("range_offset", parsed == null ? null : parsed[0]);
            rec.put("range_length", parsed == null ? null : (parsed[1] < 0 ? null : parsed[1]));

            String referer = header(httpRequest, "Referer");
            Map<String, String> audit = parseRefererQuery(referer);
            rec.put("audit_span_id", audit.get("id"));
            rec.put("audit_op", audit.get("op"));
            rec.put("audit_path", audit.get("p1"));
            rec.put("audit_filesystem_id", audit.get("fs"));
            // S3A's own per-process UUID; identifies the executor JVM without reading the OS process id
            rec.put("audit_process_id", audit.get("ps"));
            rec.put("audit_thread_exec", audit.get("t1"));
            for (String key : correlationKeys) {
                if (!key.isEmpty()) {
                    rec.put("audit_" + key, audit.get(key));
                }
            }
        }

        if (httpResponse != null) {
            rec.put("http_status", httpResponse.statusCode());
            rec.put("bytes_expected", asLong(header(httpResponse, "Content-Length")));
            rec.put("etag", header(httpResponse, "ETag"));
            rec.put("version_id", header(httpResponse, "x-amz-version-id"));
            rec.put("request_id", header(httpResponse, "x-amz-request-id"));
        } else {
            rec.put("http_status", null);
        }

        if (failure != null) {
            rec.put("error", failure.getClass().getName());
        }

        sink.write(toJson(rec));
    }

    // ---------------------------------------------------------------- parsing

    /**
     * Parses an HTTP {@code Range} header into {@code {offset, length}}.
     *
     * <p>HTTP ranges are inclusive at both ends, so {@code bytes=0-99} is 100 bytes. An open-ended range yields a
     * length of {@code -1}, which the caller reports as unknown rather than guessing the object size.
     *
     * @return {@code null} when the header is absent or not a single byte range
     */
    static long[] parseRange(String rangeHeader) {
        if (rangeHeader == null) {
            return null;
        }
        String value = rangeHeader.trim();
        if (!value.startsWith("bytes=")) {
            return null;
        }
        value = value.substring("bytes=".length()).trim();
        // multi-range requests are not issued by the readers we measure; record them as unparsed
        if (value.indexOf(',') >= 0) {
            return null;
        }
        int dash = value.indexOf('-');
        if (dash <= 0) {
            return null;
        }
        try {
            long start = Long.parseLong(value.substring(0, dash).trim());
            String endText = value.substring(dash + 1).trim();
            if (endText.isEmpty()) {
                return new long[] {start, -1L};
            }
            long endInclusive = Long.parseLong(endText);
            if (endInclusive < start) {
                return null;
            }
            return new long[] {start, endInclusive - start + 1};
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Extracts the audit fields S3A encodes in the {@code Referer} header query string.
     *
     * <p>The header looks like {@code https://audit.example.org/hadoop/1/op_open/<span>/?op=op_open&p1=s3a://...&id=...}.
     * S3A truncates the header when it grows too long, so a missing field is normal and never an error.
     */
    static Map<String, String> parseRefererQuery(String referer) {
        Map<String, String> out = new LinkedHashMap<>();
        if (referer == null) {
            return out;
        }
        int q = referer.indexOf('?');
        if (q < 0 || q == referer.length() - 1) {
            return out;
        }
        for (String pair : referer.substring(q + 1).split("&")) {
            if (pair.isEmpty()) {
                continue;
            }
            int eq = pair.indexOf('=');
            String key = eq < 0 ? pair : pair.substring(0, eq);
            String value = eq < 0 ? "" : pair.substring(eq + 1);
            out.put(urlDecode(key), urlDecode(value));
        }
        return out;
    }

    private static String urlDecode(String s) {
        try {
            return URLDecoder.decode(s, "UTF-8");
        } catch (UnsupportedEncodingException | IllegalArgumentException e) {
            return s;
        }
    }

    private static String header(SdkHttpRequest request, String name) {
        return request.firstMatchingHeader(name).orElse(null);
    }

    private static String header(SdkHttpResponse response, String name) {
        return response.firstMatchingHeader(name).orElse(null);
    }

    private static Long asLong(String s) {
        if (s == null) {
            return null;
        }
        try {
            return Long.valueOf(s.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // ------------------------------------------------------------ serialising

    static String toJson(Map<String, Object> record) {
        StringBuilder sb = new StringBuilder(512).append('{');
        boolean first = true;
        for (Map.Entry<String, Object> e : record.entrySet()) {
            if (e.getValue() == null) {
                continue;
            }
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(escape(e.getKey())).append("\":");
            Object v = e.getValue();
            if (v instanceof Number) {
                sb.append(v);
            } else if (v instanceof Boolean) {
                sb.append(v);
            } else {
                sb.append('"').append(escape(v.toString())).append('"');
            }
        }
        return sb.append('}').toString();
    }

    static String escape(String s) {
        StringBuilder sb = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                    break;
            }
        }
        return sb.toString();
    }

    // ------------------------------------------------------------------- sink

    /**
     * One NDJSON file per JVM, shared by every interceptor instance.
     *
     * <p>Writes are serialised on the writer. At the GET rates a single executor sustains against S3 (hundreds per
     * second, each costing milliseconds of network) this contention is irrelevant next to the IO being measured, and
     * it keeps the collector simple enough to reason about under executor shutdown.
     */
    static final class RecordSink {
        private static volatile RecordSink instance;

        private final Writer writer;

        RecordSink(Writer writer) {
            this.writer = writer;
        }

        static RecordSink shared() {
            RecordSink local = instance;
            if (local != null) {
                return local;
            }
            synchronized (RecordSink.class) {
                if (instance == null) {
                    instance = create();
                }
                return instance;
            }
        }

        private static RecordSink create() {
            try {
                Path path = Track2CollectorSetting.DIR.getStringValue()
                                                      .map(Paths::get)
                                                      .orElseGet(() -> Paths.get(
                                                          Track2CollectorSetting.TEMP_DIR.getStringValueOrThrow(),
                                                          "track2-io"));
                Files.createDirectories(path);
                // the executor JVM is identified by the audit "ps" field carried in every record, not by the filename
                Path file = path.resolve("track2-io-" + UUID.randomUUID() + ".ndjson");
                Writer w = new BufferedWriter(new OutputStreamWriter(
                    Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                    StandardCharsets.UTF_8), 1 << 16);
                RecordSink sink = new RecordSink(w);
                Runtime.getRuntime().addShutdownHook(new Thread(sink::close, "track2-collector-flush"));
                return sink;
            } catch (IOException | RuntimeException e) {
                // a collector that cannot open its file must still not break the job
                return new RecordSink(null);
            }
        }

        void write(String line) {
            if (writer == null) {
                return;
            }
            synchronized (this) {
                try {
                    writer.write(line);
                    writer.write('\n');
                } catch (IOException e) {
                    // drop the record
                }
            }
        }

        void close() {
            if (writer == null) {
                return;
            }
            synchronized (this) {
                try {
                    writer.flush();
                    writer.close();
                } catch (IOException e) {
                    // nothing useful to do during shutdown
                }
            }
        }
    }
}
