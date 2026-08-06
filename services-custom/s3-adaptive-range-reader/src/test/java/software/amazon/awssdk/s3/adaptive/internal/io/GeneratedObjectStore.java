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

import java.util.HashMap;
import java.util.Map;

/**
 * An {@link ObjectStore} whose bytes are a deterministic function of absolute position, so huge objects (hundreds of
 * MiB, as in real traces) can be "stored" without allocating them. The same function {@link #byteAt(long)} lets tests
 * compute the expected bytes for any range independently of the reader.
 */
public final class GeneratedObjectStore implements ObjectStore {

    private final Map<String, Long> sizes = new HashMap<>();

    private int getCount;
    private long totalGetBytes;
    private long maxSingleFetchBytes;
    private long rttNanos;
    private double bandwidthMiBps;

    public void define(String key, long size) {
        sizes.merge(key, size, Math::max);
    }

    /**
     * Inject a per-GET {@code rtt + bytes/BW} delay (see {@link SimLatency}); {@code 0}/{@code 0} disables it.
     */
    public GeneratedObjectStore latency(long rttNanos, double bandwidthMiBps) {
        this.rttNanos = rttNanos;
        this.bandwidthMiBps = bandwidthMiBps;
        return this;
    }

    /**
     * Deterministic pseudo-random byte for an absolute object position.
     */
    public static byte byteAt(long pos) {
        long x = (pos + 1) * 0x9E3779B97F4A7C15L;
        x ^= x >>> 29;
        x *= 0xBF58476D1CE4E5B9L;
        x ^= x >>> 32;
        return (byte) x;
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
    public byte[] getRange(String key, long start, long endExclusive, String expectedVersionToken) {
        Long size = sizes.get(key);
        if (size == null) {
            throw new IllegalArgumentException("no such object: " + key);
        }
        if (start < 0 || endExclusive > size || endExclusive < start) {
            throw new IndexOutOfBoundsException("bad range [" + start + "," + endExclusive + ") size=" + size);
        }
        int len = (int) (endExclusive - start);
        SimLatency.sleepNanos(SimLatency.fetchNanos(rttNanos, bandwidthMiBps, len));
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            out[i] = byteAt(start + i);
        }
        getCount++;
        totalGetBytes += len;
        maxSingleFetchBytes = Math.max(maxSingleFetchBytes, len);
        return out;
    }

    public int getCount() {
        return getCount;
    }

    public long totalGetBytes() {
        return totalGetBytes;
    }

    public long maxSingleFetchBytes() {
        return maxSingleFetchBytes;
    }
}
