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

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * In-process record of every {@code GetObject} that crossed a Track 1 S3A wrapper.
 *
 * <p>P0 is passthrough: the probe exists so the factory can prove it sits on the
 * actual Spark/S3A GET path (sync vs async, range, version, status) without
 * changing bytes. Records stay in a bounded memory ring; when
 * {@code -Dtrack1.s3a.probe.dir} is set they are also appended as NDJSON.
 */
@SdkInternalApi
public final class Track1S3aProbe {

    private static final int RING = 10_000;
    private static final Track1S3aProbe INSTANCE = new Track1S3aProbe();

    private final ConcurrentLinkedQueue<Record> ring = new ConcurrentLinkedQueue<>();
    private final AtomicInteger size = new AtomicInteger();
    private final AtomicLong syncGets = new AtomicLong();
    private final AtomicLong asyncGets = new AtomicLong();
    private final AtomicLong exceptions = new AtomicLong();
    private final Writer sink;

    private Track1S3aProbe() {
        this.sink = openSink();
    }

    public static Track1S3aProbe shared() {
        return INSTANCE;
    }

    public Record record(String client, GetObjectRequest request, long startNanos,
                         Throwable error) {
        long[] range = parseRange(request == null ? null : request.range());
        Record rec = new Record(
            client,
            request == null ? null : request.bucket(),
            request == null ? null : request.key(),
            range[0],
            range[1],
            request == null ? null : request.versionId(),
            error == null ? "ok" : "exception",
            error == null ? null : error.getClass().getName(),
            System.nanoTime() - startNanos);
        if ("async".equals(client)) {
            asyncGets.incrementAndGet();
        } else {
            syncGets.incrementAndGet();
        }
        if (error != null) {
            exceptions.incrementAndGet();
        }
        ring.add(rec);
        if (size.incrementAndGet() > RING) {
            ring.poll();
            size.decrementAndGet();
        }
        write(rec);
        return rec;
    }

    public List<Record> snapshot() {
        return Collections.unmodifiableList(new ArrayList<>(ring));
    }

    public long syncGets() {
        return syncGets.get();
    }

    public long asyncGets() {
        return asyncGets.get();
    }

    public long exceptions() {
        return exceptions.get();
    }

    public void reset() {
        ring.clear();
        size.set(0);
        syncGets.set(0);
        asyncGets.set(0);
        exceptions.set(0);
    }

    static long[] parseRange(String header) {
        long[] none = { -1L, -1L };
        if (header == null) {
            return none;
        }
        String value = header.trim().toLowerCase(Locale.ROOT);
        if (!value.startsWith("bytes=") || value.indexOf(',') >= 0) {
            return none;
        }
        String spec = value.substring("bytes=".length());
        int dash = spec.indexOf('-');
        if (dash <= 0) {
            return none;
        }
        try {
            long start = Long.parseLong(spec.substring(0, dash));
            String endText = spec.substring(dash + 1);
            long end = endText.isEmpty() ? -1L : Long.parseLong(endText);
            if (end >= 0 && end < start) {
                return none;
            }
            return new long[] { start, end };
        } catch (NumberFormatException e) {
            return none;
        }
    }

    private static Writer openSink() {
        String dir = System.getProperty("track1.s3a.probe.dir",
                                        System.getenv("TRACK1_S3A_PROBE_DIR"));
        if (dir == null || dir.isEmpty()) {
            return null;
        }
        try {
            Path folder = Paths.get(dir);
            Files.createDirectories(folder);
            Path file = folder.resolve("track1-s3a-probe-" + ProcessHandleSupport.pid() + ".ndjson");
            return new BufferedWriter(new OutputStreamWriter(
                Files.newOutputStream(file, StandardOpenOption.CREATE, StandardOpenOption.APPEND),
                StandardCharsets.UTF_8));
        } catch (IOException e) {
            return null;
        }
    }

    private void write(Record rec) {
        if (sink == null) {
            return;
        }
        try {
            sink.write(rec.toJson());
            sink.write('\n');
            sink.flush();
        } catch (IOException ignored) {
            // Losing a probe line must never fail the GET being measured.
        }
    }

    public static final class Record {
        public final String client;
        public final String bucket;
        public final String key;
        public final long rangeStart;
        public final long rangeEndInclusive;
        public final String versionId;
        public final String status;
        public final String error;
        public final long latencyNanos;

        Record(String client, String bucket, String key, long rangeStart, long rangeEndInclusive,
               String versionId, String status, String error, long latencyNanos) {
            this.client = client;
            this.bucket = bucket;
            this.key = key;
            this.rangeStart = rangeStart;
            this.rangeEndInclusive = rangeEndInclusive;
            this.versionId = versionId;
            this.status = status;
            this.error = error;
            this.latencyNanos = latencyNanos;
        }

        String toJson() {
            StringBuilder sb = new StringBuilder(256).append('{');
            field(sb, "client", client, true);
            field(sb, "bucket", bucket, false);
            field(sb, "key", key, false);
            if (rangeStart >= 0) {
                sb.append(",\"range_start\":").append(rangeStart);
            }
            if (rangeEndInclusive >= 0) {
                sb.append(",\"range_end\":").append(rangeEndInclusive);
            }
            field(sb, "version_id", versionId, false);
            field(sb, "status", status, false);
            field(sb, "error", error, false);
            sb.append(",\"latency_ns\":").append(latencyNanos);
            return sb.append('}').toString();
        }

        private static void field(StringBuilder sb, String name, String value, boolean first) {
            if (value == null) {
                return;
            }
            if (!first) {
                sb.append(',');
            }
            sb.append('"').append(name).append("\":\"");
            for (int i = 0; i < value.length(); i++) {
                char c = value.charAt(i);
                if (c == '"' || c == '\\') {
                    sb.append('\\');
                }
                if (c >= 32) {
                    sb.append(c);
                }
            }
            sb.append('"');
        }
    }

    /** Java 8 stand-in: ProcessHandle is Java 9+. */
    private static final class ProcessHandleSupport {
        static long pid() {
            try {
                String name = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
                int at = name.indexOf('@');
                return at < 0 ? 0L : Long.parseLong(name.substring(0, at));
            } catch (Exception e) {
                return 0L;
            }
        }
    }
}
