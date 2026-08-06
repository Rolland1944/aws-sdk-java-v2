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

import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.s3.adaptive.internal.AdaptiveRangeReaderImpl;
import software.amazon.awssdk.s3.adaptive.internal.PassthroughRangeReader;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.io.S3ClientObjectStore;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * A synchronous, seekable reader over a single S3 object that adapts its IO access pattern per read (Track 1, S2).
 *
 * <p>When {@link Builder#enabled(boolean) enabled}, each logical read is observed by the offline-trained policy
 * selector (feature window + decision tree + hysteresis) and served by the chosen policy's executor over a
 * byte-budgeted page cache. When disabled (the default), reads pass through as exact demand ranged GETs, byte-for-byte
 * identical to a plain {@code getObject} range request.
 *
 * <p>Instances are <b>not</b> thread-safe: use one reader per stream.
 *
 * <p>This is experimental and opt-in; APIs may change.
 */
@SdkPublicApi
public interface AdaptiveRangeReader extends SdkAutoCloseable {

    /**
     * Total size of the object in bytes (pinned at construction).
     */
    long size();

    /**
     * Read up to {@code length} bytes starting at absolute {@code position} into {@code dst}. Does not move the
     * stream cursor.
     *
     * @return the number of bytes read ({@code <= length}, fewer only near EOF), or {@code -1} if {@code position}
     *         is at or past the end of the object.
     */
    int read(long position, byte[] dst, int offset, int length);

    /**
     * Read up to {@code length} bytes at the current cursor and advance it by the number of bytes read.
     *
     * @return bytes read, or {@code -1} at EOF.
     */
    int read(byte[] dst, int offset, int length);

    /**
     * Move the stream cursor to an absolute position.
     */
    void seek(long position);

    /**
     * The current stream cursor position.
     */
    long position();

    /**
     * A snapshot of this reader's IO metrics.
     */
    ReaderMetrics metrics();

    /**
     * The current policy after hysteresis, or {@code null} if none has been chosen yet (e.g. passthrough or no reads).
     */
    PolicyName currentPolicy();

    static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link AdaptiveRangeReader}. Address an object via an {@link S3Client} + bucket + key; the reader
     * heads the object once to pin its size and version.
     */
    @SdkPublicApi
    final class Builder {

        private static final long DEFAULT_CACHE_BUDGET_BYTES = 64L * 1024 * 1024;
        private static final long DEFAULT_MAX_SINGLE_FETCH_BYTES = 8L * 1024 * 1024;

        private S3Client s3Client;
        private String bucket;
        private String key;
        private String versionId;
        private boolean enabled;
        private long cacheBudgetBytes = DEFAULT_CACHE_BUDGET_BYTES;
        private long maxSingleFetchBytes = DEFAULT_MAX_SINGLE_FETCH_BYTES;
        private String appId;

        private Builder() {
        }

        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        /**
         * Optional S3 object version id (for versioned buckets).
         */
        public Builder versionId(String versionId) {
            this.versionId = versionId;
            return this;
        }

        /**
         * Turn adaptive policy selection on. Off (default) means passthrough demand reads.
         */
        public Builder enabled(boolean enabled) {
            this.enabled = enabled;
            return this;
        }

        /**
         * Hard cap on the page cache size in bytes (LRU eviction beyond this).
         */
        public Builder cacheBudgetBytes(long cacheBudgetBytes) {
            this.cacheBudgetBytes = cacheBudgetBytes;
            return this;
        }

        /**
         * Hard cap on a single ranged GET; a policy that would over-read past this falls back to an exact demand
         * fetch for that read.
         */
        public Builder maxSingleFetchBytes(long maxSingleFetchBytes) {
            this.maxSingleFetchBytes = maxSingleFetchBytes;
            return this;
        }

        /**
         * Optional organization dimension for metrics/priors. In SDK 2.25.70 there is no native application id, and
         * caller identity ({@code AwsCredentialsIdentity#accessKeyId()}/{@code accountId()}) is not exposed off an
         * {@link S3Client}, unstable under STS, or too coarse; supply a stable id here (e.g. derived from
         * {@code accountId}) if you want a per-application dimension. Per-app priors persistence is deferred (S3+).
         */
        public Builder appId(String appId) {
            this.appId = appId;
            return this;
        }

        public AdaptiveRangeReader build() {
            if (s3Client == null) {
                throw new IllegalArgumentException("s3Client is required");
            }
            if (bucket == null || key == null) {
                throw new IllegalArgumentException("bucket and key are required");
            }
            ObjectStore store = new S3ClientObjectStore(s3Client, bucket, versionId);
            if (!enabled) {
                return new PassthroughRangeReader(store, key);
            }
            return new AdaptiveRangeReaderImpl(store, key, cacheBudgetBytes, maxSingleFetchBytes, appId);
        }
    }
}
