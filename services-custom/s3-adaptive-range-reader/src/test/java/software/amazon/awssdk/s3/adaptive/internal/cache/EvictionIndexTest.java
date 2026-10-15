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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.junit.jupiter.api.Test;

class EvictionIndexTest {

    private static final Predicate<String> ANY = k -> true;

    private static List<String> plan(EvictionIndex<String> index, long needed, Predicate<String> ok) {
        List<String> out = new ArrayList<String>();
        index.plan(needed, ok, out);
        return out;
    }

    /** Evict one victim the way AppCache does: plan, then remove. */
    private static String evictOne(EvictionIndex<String> index) {
        List<String> v = plan(index, 1L, ANY);
        if (v.isEmpty()) {
            return null;
        }
        index.remove(v.get(0), true);
        return v.get(0);
    }

    // ---- SIEVE ----

    @Test
    void sieveEvictsOldestUnvisitedAndKeepsOrder() {
        SieveIndex<String> s = new SieveIndex<String>();
        s.insert("a", 10);
        s.insert("b", 10);
        s.insert("c", 10);

        assertThat(evictOne(s)).isEqualTo("a");
        assertThat(evictOne(s)).isEqualTo("b");
        assertThat(evictOne(s)).isEqualTo("c");
        assertThat(evictOne(s)).isNull();
        assertThat(s.size()).isZero();
    }

    @Test
    void sieveVisitedEntryIsSkippedOnceAndNotMoved() {
        SieveIndex<String> s = new SieveIndex<String>();
        s.insert("a", 10);
        s.insert("b", 10);
        s.insert("c", 10);
        s.hit("a");

        // hand starts at oldest "a": visited -> cleared and skipped, "b" is the victim
        assertThat(evictOne(s)).isEqualTo("b");
        // hand continues toward newer entries: "c" is next, not "a"
        assertThat(evictOne(s)).isEqualTo("c");
        // wrapped around: "a" lost its visited bit and is now evictable
        assertThat(evictOne(s)).isEqualTo("a");
    }

    @Test
    void sieveNewEntriesInsertedBehindTheHandAreNotSweptFirst() {
        SieveIndex<String> s = new SieveIndex<String>();
        s.insert("a", 10);
        s.insert("b", 10);
        s.insert("c", 10);
        assertThat(evictOne(s)).isEqualTo("a");   // hand now at "b"
        s.insert("d", 10);                        // head side, ahead of the hand
        assertThat(evictOne(s)).isEqualTo("b");
        assertThat(evictOne(s)).isEqualTo("c");
        assertThat(evictOne(s)).isEqualTo("d");
    }

    @Test
    void sieveSkipsNonEvictableWithoutClearingTheirVisitedBit() {
        SieveIndex<String> s = new SieveIndex<String>();
        s.insert("a", 10);
        s.insert("b", 10);
        s.hit("a");
        Set<String> pinned = new HashSet<String>(Collections.singleton("a"));
        Predicate<String> notPinned = k -> !pinned.contains(k);

        assertThat(plan(s, 1, notPinned)).containsExactly("b");
        s.remove("b", true);
        pinned.clear();
        // "a" stayed visited while pinned: first sweep clears it, second selects it
        assertThat(plan(s, 1, ANY)).containsExactly("a");
    }

    @Test
    void sieveAllPinnedYieldsNothing() {
        SieveIndex<String> s = new SieveIndex<String>();
        s.insert("a", 10);
        s.insert("b", 10);
        assertThat(plan(s, 5, k -> false)).isEmpty();
        assertThat(s.size()).isEqualTo(2);
    }

    @Test
    void sievePlanCollectsEnoughBytesWithoutDuplicates() {
        SieveIndex<String> s = new SieveIndex<String>();
        for (String k : new String[] {"a", "b", "c", "d"}) {
            s.insert(k, 10);
        }
        s.hit("a");
        s.hit("b");
        List<String> v = plan(s, 40, ANY);
        assertThat(v).containsExactlyInAnyOrder("a", "b", "c", "d");
        assertThat(new HashSet<String>(v)).hasSize(4);
    }

    @Test
    void sieveRemovingTheHandNodeAdvancesTheHand() {
        SieveIndex<String> s = new SieveIndex<String>();
        s.insert("a", 10);
        s.insert("b", 10);
        s.insert("c", 10);
        s.hit("a");
        s.hit("b");
        assertThat(evictOne(s)).isEqualTo("c"); // sweep cleared a,b and took c; hand -> newer side/wrap
        s.remove("a", false);                   // invalidate (not an eviction)
        assertThat(s.size()).isEqualTo(1);
        assertThat(evictOne(s)).isEqualTo("b");
        assertThat(evictOne(s)).isNull();
    }

    // ---- GDSF ----

    @Test
    void gdsfPrefersEvictingLargeLowFrequencyEntries() {
        GdsfIndex<String> g = new GdsfIndex<String>(bytes -> 100.0);
        g.insert("small", 10);   // 100/10  = 10
        g.insert("large", 1000); // 100/1000 = 0.1
        assertThat(plan(g, 1, ANY)).containsExactly("large");
    }

    @Test
    void gdsfFrequencyRaisesPriority() {
        GdsfIndex<String> g = new GdsfIndex<String>(bytes -> 1.0);
        g.insert("a", 10);
        g.insert("b", 10);
        g.hit("a");
        g.hit("a");
        assertThat(plan(g, 1, ANY)).containsExactly("b");
    }

    @Test
    void gdsfTieBreaksByAge() {
        GdsfIndex<String> g = new GdsfIndex<String>(bytes -> 1.0);
        g.insert("a", 10);
        g.insert("b", 10);
        assertThat(plan(g, 1, ANY)).containsExactly("a");
    }

    @Test
    void gdsfInflationAgesStaleEntries() {
        GdsfIndex<String> g = new GdsfIndex<String>(bytes -> 1.0);
        g.insert("old", 10);
        for (int i = 0; i < 5; i++) {
            g.hit("old");               // freq 6 -> priority 0.6
        }
        // churn: each evicted one-hit entry raises L to its priority
        for (int i = 0; i < 10; i++) {
            String key = "t" + i;
            g.insert(key, 10);
            g.hit(key);                 // priority = L + 0.2, then eviction lifts L by that
            g.remove(key, true);
        }
        assertThat(g.inflation()).isGreaterThan(0.6);
        g.insert("new", 10);            // L + 0.1 > old's stale 0.6
        assertThat(plan(g, 1, ANY)).containsExactly("old");
    }

    @Test
    void gdsfReplacementDoesNotInflate() {
        GdsfIndex<String> g = new GdsfIndex<String>(bytes -> 1.0);
        g.insert("a", 10);
        g.hit("a");
        g.remove("a", false);
        assertThat(g.inflation()).isZero();
    }

    @Test
    void gdsfSkipsNonEvictableAndAccumulatesBytes() {
        GdsfIndex<String> g = new GdsfIndex<String>(bytes -> 1.0);
        g.insert("a", 10);
        g.insert("b", 20);
        g.insert("c", 30);
        List<String> out = new ArrayList<String>();
        long got = g.plan(45, k -> !k.equals("a"), out);
        // priorities 1/size: c(0.033) < b(0.05) < a(0.1)
        assertThat(out).containsExactly("c", "b");
        assertThat(got).isEqualTo(50);
        // selection does not mutate
        assertThat(g.size()).isEqualTo(3);
        assertThat(plan(g, 1, ANY)).containsExactly("c");
    }
}
