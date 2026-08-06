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

package software.amazon.awssdk.s3.adaptive.internal;

import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Immutable, process-level tunables resolved by {@code AdaptiveReaderRuntime.Builder} and shared with every
 * {@code AppContext} and reader it creates.
 */
@SdkInternalApi
public final class RuntimeConfig {

    private final long globalCacheBytes;
    private final long perAppReservedBytes;
    private final int maxConcurrentGets;
    private final long perAppInflightBytes;
    private final int prefetchDepth;
    private final long prefetchBlockSize;
    private final long maxSingleFetchBytes;

    private RuntimeConfig(Builder b) {
        this.globalCacheBytes = b.globalCacheBytes;
        this.perAppReservedBytes = b.perAppReservedBytes;
        this.maxConcurrentGets = b.maxConcurrentGets;
        this.perAppInflightBytes = b.perAppInflightBytes;
        this.prefetchDepth = b.prefetchDepth;
        this.prefetchBlockSize = b.prefetchBlockSize;
        this.maxSingleFetchBytes = b.maxSingleFetchBytes;
    }

    public static Builder builder() {
        return new Builder();
    }

    public long globalCacheBytes() {
        return globalCacheBytes;
    }

    public long perAppReservedBytes() {
        return perAppReservedBytes;
    }

    public int maxConcurrentGets() {
        return maxConcurrentGets;
    }

    public long perAppInflightBytes() {
        return perAppInflightBytes;
    }

    public int prefetchDepth() {
        return prefetchDepth;
    }

    public long prefetchBlockSize() {
        return prefetchBlockSize;
    }

    public long maxSingleFetchBytes() {
        return maxSingleFetchBytes;
    }

    /**
     * Builder with sane defaults for the tunables.
     */
    @SdkInternalApi
    public static final class Builder {

        private long globalCacheBytes = 256L * 1024 * 1024;
        private long perAppReservedBytes = 32L * 1024 * 1024;
        private int maxConcurrentGets = 16;
        private long perAppInflightBytes = 32L * 1024 * 1024;
        private int prefetchDepth = 1;
        private long prefetchBlockSize = 1024L * 1024;
        private long maxSingleFetchBytes = 8L * 1024 * 1024;

        private Builder() {
        }

        public Builder globalCacheBytes(long globalCacheBytes) {
            this.globalCacheBytes = globalCacheBytes;
            return this;
        }

        public Builder perAppReservedBytes(long perAppReservedBytes) {
            this.perAppReservedBytes = perAppReservedBytes;
            return this;
        }

        public Builder maxConcurrentGets(int maxConcurrentGets) {
            this.maxConcurrentGets = maxConcurrentGets;
            return this;
        }

        public Builder perAppInflightBytes(long perAppInflightBytes) {
            this.perAppInflightBytes = perAppInflightBytes;
            return this;
        }

        public Builder prefetchDepth(int prefetchDepth) {
            this.prefetchDepth = prefetchDepth;
            return this;
        }

        public Builder prefetchBlockSize(long prefetchBlockSize) {
            this.prefetchBlockSize = prefetchBlockSize;
            return this;
        }

        public Builder maxSingleFetchBytes(long maxSingleFetchBytes) {
            this.maxSingleFetchBytes = maxSingleFetchBytes;
            return this;
        }

        public RuntimeConfig build() {
            return new RuntimeConfig(this);
        }
    }
}
