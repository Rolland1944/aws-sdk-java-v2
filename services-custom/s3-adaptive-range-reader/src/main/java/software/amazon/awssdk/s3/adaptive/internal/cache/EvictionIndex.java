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

package software.amazon.awssdk.s3.adaptive.internal.cache;

import java.util.List;
import java.util.function.Predicate;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Victim-selection structure for the baseline replacement policies compared against the W-TinyLFU path (GDSF, SIEVE).
 * It only tracks keys and byte sizes; {@link AppCache} still owns the blocks, pins, budget and request pieces. Not
 * thread-safe: callers hold the cache monitor.
 */
@SdkInternalApi
interface EvictionIndex<K> {

    /** Register a newly cached key (an existing entry for the same key is replaced and treated as new). */
    void insert(K key, long bytes);

    /** Record a cache hit on {@code key}; unknown keys are ignored. */
    void hit(K key);

    /**
     * Forget {@code key}.
     *
     * @param evicted {@code true} when removed to make room (policies may age on it), {@code false} when the entry
     *                was only replaced or invalidated
     */
    void remove(K key, boolean evicted);

    /**
     * Select victims totalling at least {@code needed} bytes, in eviction order, among keys accepted by
     * {@code evictable}. Appends to {@code out} and returns the bytes selected, which may be below {@code needed}
     * when too few keys are evictable. Selected keys are not removed; the caller removes them via
     * {@link #remove(Object, boolean)}.
     */
    long plan(long needed, Predicate<K> evictable, List<K> out);

    int size();
}
