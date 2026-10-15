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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * SIEVE (Zhang et al., NSDI'24): one FIFO list plus a "hand" that sweeps from the oldest entry toward the newest.
 * A hit only sets the entry's visited bit. Eviction moves the hand: visited entries get their bit cleared and are
 * kept in place (not moved), the first unvisited entry is evicted. New entries go to the head with visited=false.
 *
 * <p>Entries that the caller cannot evict (pinned or excluded) are skipped without touching their visited bit.
 * {@link #plan} advances the hand and clears visited bits as the real sweep would; if the caller then abandons the
 * plan, those side effects stay (they only make a few entries easier to evict).
 */
@SdkInternalApi
final class SieveIndex<K> implements EvictionIndex<K> {
    private final Map<K, Node<K>> nodes = new HashMap<K, Node<K>>();
    // head = newest, tail = oldest. The hand moves tail -> head via prev; null means "start at tail".
    private Node<K> head;
    private Node<K> tail;
    private Node<K> hand;

    @Override
    public void insert(K key, long bytes) {
        remove(key, false);
        if (bytes <= 0L) {
            return;
        }
        Node<K> node = new Node<K>(key, bytes);
        node.next = head;
        if (head != null) {
            head.prev = node;
        }
        head = node;
        if (tail == null) {
            tail = node;
        }
        nodes.put(key, node);
    }

    @Override
    public void hit(K key) {
        Node<K> node = nodes.get(key);
        if (node != null) {
            node.visited = true;
        }
    }

    @Override
    public void remove(K key, boolean evicted) {
        Node<K> node = nodes.remove(key);
        if (node == null) {
            return;
        }
        if (hand == node) {
            hand = node.prev;
        }
        if (node.prev != null) {
            node.prev.next = node.next;
        } else {
            head = node.next;
        }
        if (node.next != null) {
            node.next.prev = node.prev;
        } else {
            tail = node.prev;
        }
        node.prev = null;
        node.next = null;
    }

    @Override
    public long plan(long needed, Predicate<K> evictable, List<K> out) {
        long got = 0L;
        if (nodes.isEmpty() || needed <= 0L) {
            return got;
        }
        Set<K> chosen = new HashSet<K>();
        Node<K> cursor = hand != null ? hand : tail;
        // Two full sweeps are enough: the first clears every visited bit of evictable entries.
        long steps = 2L * nodes.size() + 1L;
        while (got < needed && steps-- > 0L && cursor != null) {
            Node<K> current = cursor;
            cursor = current.prev != null ? current.prev : tail;
            if (chosen.contains(current.key) || !evictable.test(current.key)) {
                continue;
            }
            if (current.visited) {
                current.visited = false;
                continue;
            }
            chosen.add(current.key);
            out.add(current.key);
            got += current.bytes;
        }
        // Real SIEVE leaves the hand on the predecessor of the last victim. If that is itself a chosen victim
        // (full wrap) or the sweep ran dry, restart from the tail next time.
        hand = cursor != null && !chosen.contains(cursor.key) ? cursor : null;
        return got;
    }

    @Override
    public int size() {
        return nodes.size();
    }

    private static final class Node<K> {
        private final K key;
        private final long bytes;
        private boolean visited;
        private Node<K> prev;
        private Node<K> next;

        private Node(K key, long bytes) {
            this.key = key;
            this.bytes = bytes;
        }
    }
}
