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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.s3.adaptive.internal.io.GeneratedObjectStore;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.S3Exception;

/**
 * Explicitly-run provisioner for deterministic objects required by a trace-backed MinIO benchmark.
 *
 * <p>This class is intentionally not named {@code *Test}, so it does not run in normal Surefire discovery. Run it
 * only after exporting the {@link MinioEnvironment} variables:
 *
 * <pre>
 * mvn -q -pl services-custom/s3-adaptive-range-reader -Dtest=MinioTraceObjectProvisioner test \
 *     -Ds3arr.trace=/absolute/path/to/mixed_holdout.csv
 * </pre>
 */
class MinioTraceObjectProvisioner {

    private static final long PART_BYTES = 16L * 1024 * 1024;

    @Test
    void provision() throws IOException {
        Path trace = requiredTrace();
        Map<String, Long> objects = inventory(trace);
        assertThat(objects).as("objects in trace").isNotEmpty();

        MinioEnvironment minio = MinioEnvironment.fromEnvironment();
        try (S3Client s3 = minio.syncClient()) {
            ensureBucket(s3, minio.bucket());
            List<ManifestEntry> manifest = new ArrayList<>();
            for (Map.Entry<String, Long> object : objects.entrySet()) {
                Long existing = existingSize(s3, minio.bucket(), object.getKey());
                if (existing != null && existing >= object.getValue()) {
                    System.out.println("[provision] skip " + object.getKey() + " (" + existing + " bytes)");
                    manifest.add(new ManifestEntry(object.getKey(), object.getValue(), existing, "existing"));
                    continue;
                }
                upload(s3, minio.bucket(), object.getKey(), object.getValue());
                Long verified = existingSize(s3, minio.bucket(), object.getKey());
                if (verified == null || verified != object.getValue()) {
                    throw new IllegalStateException("HEAD verification failed for " + object.getKey()
                                                    + ": expected " + object.getValue() + ", got " + verified);
                }
                manifest.add(new ManifestEntry(object.getKey(), object.getValue(), verified, "uploaded"));
            }
            writeManifest(trace, minio.bucket(), manifest);
            System.out.println("[provision] ready: " + objects.size() + " objects in bucket " + minio.bucket());
        }
    }

    private static void ensureBucket(S3Client s3, String bucket) {
        try {
            s3.headBucket(HeadBucketRequest.builder().bucket(bucket).build());
        } catch (NoSuchBucketException e) {
            s3.createBucket(builder -> builder.bucket(bucket));
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                s3.createBucket(builder -> builder.bucket(bucket));
            } else {
                throw e;
            }
        }
    }

    private static Long existingSize(S3Client s3, String bucket, String key) {
        try {
            HeadObjectResponse head = s3.headObject(builder -> builder.bucket(bucket).key(key));
            return head.contentLength();
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return null;
            }
            throw e;
        }
    }

    private static void upload(S3Client s3, String bucket, String key, long size) {
        System.out.println("[provision] upload " + key + " (" + size + " bytes)");
        CreateMultipartUploadResponse created = s3.createMultipartUpload(builder -> builder.bucket(bucket).key(key)
                                                                                       .contentType(
                                                                                           "application/octet-stream"));
        List<CompletedPart> parts = new ArrayList<>();
        try {
            for (long start = 0L, part = 1L; start < size; start += PART_BYTES, part++) {
                long length = Math.min(PART_BYTES, size - start);
                int partNumber = Math.toIntExact(part);
                final long partStart = start;
                final long partLength = length;
                String eTag = s3.uploadPart(builder -> builder.bucket(bucket)
                                                              .key(key)
                                                              .uploadId(created.uploadId())
                                                              .partNumber(partNumber),
                                             RequestBody.fromContentProvider(
                                                 () -> new DeterministicInputStream(partStart, partLength), partLength,
                                                 "application/octet-stream"))
                              .eTag();
                parts.add(CompletedPart.builder().partNumber(partNumber).eTag(eTag).build());
            }
            s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                                                                             .bucket(bucket)
                                                                             .key(key)
                                                                             .uploadId(created.uploadId())
                                                                             .multipartUpload(CompletedMultipartUpload
                                                                                                  .builder()
                                                                                                  .parts(parts)
                                                                                                  .build())
                                                                             .build());
        } catch (RuntimeException e) {
            s3.abortMultipartUpload(builder -> builder.bucket(bucket).key(key).uploadId(created.uploadId()));
            throw e;
        }
    }

    private static Map<String, Long> inventory(Path trace) throws IOException {
        Map<String, Long> result = new TreeMap<>();
        try (BufferedReader reader = Files.newBufferedReader(trace, StandardCharsets.UTF_8)) {
            reader.readLine();
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String[] fields = line.split(",");
                String key = fields[1];
                long offset = Long.parseLong(fields[2]);
                long length = Long.parseLong(fields[3]);
                long fileSize = Long.parseLong(fields[4]);
                result.merge(key, Math.max(fileSize, offset + length), Math::max);
            }
        }
        return result;
    }

    private static Path requiredTrace() {
        String raw = System.getProperty("s3arr.trace");
        if (raw == null || raw.trim().isEmpty()) {
            throw new IllegalArgumentException("Set -Ds3arr.trace=/absolute/path/to/trace.csv when provisioning");
        }
        Path trace = Paths.get(raw).toAbsolutePath();
        if (!Files.isRegularFile(trace)) {
            throw new IllegalArgumentException("Trace does not exist: " + trace);
        }
        return trace;
    }

    private static void writeManifest(Path trace, String bucket, List<ManifestEntry> entries) throws IOException {
        entries.sort(Comparator.comparing(entry -> entry.key));
        Path dir = Paths.get("target", "s3arr-minio");
        Files.createDirectories(dir);
        Path manifest = dir.resolve("manifest.json");
        StringBuilder json = new StringBuilder();
        json.append("{\n  \"trace\": \"").append(escape(trace.toString())).append("\",\n");
        json.append("  \"bucket\": \"").append(escape(bucket)).append("\",\n  \"objects\": [\n");
        for (int i = 0; i < entries.size(); i++) {
            ManifestEntry entry = entries.get(i);
            json.append("    {\"key\": \"").append(escape(entry.key)).append("\", \"requiredBytes\": ")
                .append(entry.requiredBytes).append(", \"actualBytes\": ").append(entry.actualBytes)
                .append(", \"action\": \"").append(entry.action).append("\"}");
            json.append(i + 1 == entries.size() ? "\n" : ",\n");
        }
        json.append("  ]\n}\n");
        Files.write(manifest, json.toString().getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
        System.out.println("[provision] wrote " + manifest.toAbsolutePath());
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private static final class ManifestEntry {
        private final String key;
        private final long requiredBytes;
        private final long actualBytes;
        private final String action;

        private ManifestEntry(String key, long requiredBytes, long actualBytes, String action) {
            this.key = key;
            this.requiredBytes = requiredBytes;
            this.actualBytes = actualBytes;
            this.action = action;
        }
    }

    private static final class DeterministicInputStream extends InputStream {
        private final long endExclusive;
        private long position;

        private DeterministicInputStream(long start, long length) {
            this.position = start;
            this.endExclusive = start + length;
        }

        @Override
        public int read() {
            if (position >= endExclusive) {
                return -1;
            }
            return GeneratedObjectStore.byteAt(position++) & 0xff;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) {
            if (position >= endExclusive) {
                return -1;
            }
            int count = (int) Math.min(length, endExclusive - position);
            for (int i = 0; i < count; i++) {
                bytes[offset + i] = GeneratedObjectStore.byteAt(position++);
            }
            return count;
        }
    }
}
