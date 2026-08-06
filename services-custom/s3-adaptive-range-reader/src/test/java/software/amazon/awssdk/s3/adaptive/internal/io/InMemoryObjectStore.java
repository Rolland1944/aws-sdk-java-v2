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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Deterministic in-memory {@link ObjectStore} test double. Records every ranged GET (count, total bytes, widest
 * single fetch) so tests can assert both correctness and IO shape without touching S3.
 */
public final class InMemoryObjectStore implements ObjectStore {

    private final Map<String, byte[]> objects = new HashMap<>();
    private final Map<String, Integer> versions = new HashMap<>();

    private int getCount;
    private long totalGetBytes;
    private long maxSingleFetchBytes;

    public void put(String key, byte[] data) {
        objects.put(key, data.clone());
        versions.merge(key, 1, Integer::sum);
    }

    private String versionToken(String key) {
        return key + "#v" + versions.getOrDefault(key, 0);
    }

    @Override
    public ObjectMeta head(String key) {
        byte[] data = objects.get(key);
        if (data == null) {
            throw new IllegalArgumentException("no such object: " + key);
        }
        return new ObjectMeta(data.length, versionToken(key));
    }

    @Override
    public byte[] getRange(String key, long start, long endExclusive, String expectedVersionToken) {
        byte[] data = objects.get(key);
        if (data == null) {
            throw new IllegalArgumentException("no such object: " + key);
        }
        if (expectedVersionToken != null && !expectedVersionToken.equals(versionToken(key))) {
            throw new ObjectChangedException("version changed for " + key);
        }
        if (start < 0 || endExclusive > data.length || endExclusive < start) {
            throw new IndexOutOfBoundsException("bad range [" + start + "," + endExclusive + ") size=" + data.length);
        }
        int len = (int) (endExclusive - start);
        getCount++;
        totalGetBytes += len;
        maxSingleFetchBytes = Math.max(maxSingleFetchBytes, len);
        return Arrays.copyOfRange(data, (int) start, (int) endExclusive);
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
