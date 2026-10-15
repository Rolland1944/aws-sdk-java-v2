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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.function.LongToDoubleFunction;
import java.util.function.Predicate;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Greedy-Dual-Size-Frequency (Cherkasova 1998). Each resident entry has priority
 * {@code H = L + freq * cost / size}; the lowest H is evicted and the inflation value {@code L} is raised to the
 * evicted H, which ages entries that stopped being hit without any periodic sweep. {@code freq} is the number of
 * hits while resident (reset on eviction). Always admits; admission is left to the caller.
 */
@SdkInternalApi
final class GdsfIndex<K> implements EvictionIndex<K> {
    private static final long MAX_FREQ = 1L << 20;

    private final LongToDoubleFunction cost;
    private final Map<K, Node<K>> nodes = new HashMap<K, Node<K>>();
    private final TreeSet<Node<K>> order = new TreeSet<Node<K>>(GdsfIndex::compare);
    private double inflation;
    private long seq;

    /**
     * @param cost refetch cost of an entry of the given byte size; any consistent unit
     */
    GdsfIndex(LongToDoubleFunction cost) {
        this.cost = cost;
    }

    @Override
    public void insert(K key, long bytes) {
        remove(key, false);
        if (bytes <= 0L) {
            return;
        }
        Node<K> node = new Node<K>(key, bytes);
        node.freq = 1L;
        reprice(node);
        nodes.put(key, node);
        order.add(node);
    }

    @Override
    public void hit(K key) {
        Node<K> node = nodes.get(key);
        if (node == null) {
            return;
        }
        order.remove(node);
        node.freq = Math.min(MAX_FREQ, node.freq + 1L);
        reprice(node);
        order.add(node);
    }

    @Override
    public void remove(K key, boolean evicted) {
        Node<K> node = nodes.remove(key);
        if (node == null) {
            return;
        }
        order.remove(node);
        if (evicted && node.priority > inflation) {
            inflation = node.priority;
        }
    }

    @Override
    public long plan(long needed, Predicate<K> evictable, List<K> out) {
        long got = 0L;
        for (Node<K> node : order) {
            if (got >= needed) {
                break;
            }
            if (evictable.test(node.key)) {
                out.add(node.key);
                got += node.bytes;
            }
        }
        return got;
    }

    @Override
    public int size() {
        return nodes.size();
    }

    double inflation() {
        return inflation;
    }

    private void reprice(Node<K> node) {
        double c = Math.max(0.0, cost.applyAsDouble(node.bytes));
        node.priority = inflation + node.freq * c / node.bytes;
        node.seq = ++seq;
    }

    private static int compare(Node<?> a, Node<?> b) {
        int c = Double.compare(a.priority, b.priority);
        if (c != 0) {
            return c;
        }
        return Long.compare(a.seq, b.seq);
    }

    private static final class Node<K> {
        private final K key;
        private final long bytes;
        private long freq;
        private double priority;
        private long seq;

        private Node(K key, long bytes) {
            this.key = key;
            this.bytes = bytes;
        }
    }
}
