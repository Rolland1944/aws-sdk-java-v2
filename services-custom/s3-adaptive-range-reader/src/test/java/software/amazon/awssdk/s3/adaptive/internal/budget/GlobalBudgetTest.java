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

package software.amazon.awssdk.s3.adaptive.internal.budget;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class GlobalBudgetTest {

    /**
     * A stand-in cache that can always free the requested LRU bytes (models a cache holding many blocks). Records how
     * much it was asked to free so the test can assert who was evicted.
     */
    private static final class FakeCache implements CacheEvictor {
        private long freed;

        @Override
        public long evictLru(long bytes) {
            freed += bytes;
            return bytes;
        }
    }

    @Test
    void reserveIsAlwaysAvailableAndBorrowedSpaceIsReclaimedFromOthers() {
        GlobalBudget budget = new GlobalBudget(1000);
        FakeCache a = new FakeCache();
        FakeCache b = new FakeCache();
        budget.register("A", 400, a);
        budget.register("B", 400, b);

        // A uses its reserve, then borrows into the idle pool.
        assertThat(budget.acquire("A", 400)).isTrue();
        assertThat(budget.acquire("A", 400)).isTrue(); // borrow 400 (usage 800)
        assertThat(budget.usage("A")).isEqualTo(800);

        // B now claims its reserve; the deficit is reclaimed from A's borrowed bytes, not from A's reserve.
        assertThat(budget.acquire("B", 400)).isTrue();
        assertThat(budget.usage("B")).isEqualTo(400);
        assertThat(budget.usage("A")).isEqualTo(600);   // dropped, but never below its 400 reserve
        assertThat(a.freed).isEqualTo(200);             // only the borrowed portion was evicted
        assertThat(b.freed).isEqualTo(0);               // B's data was never touched by A
        assertThat(budget.totalUsage()).isLessThanOrEqualTo(1000);
    }

    @Test
    void globalCeilingIsNeverExceededAndPeerReserveIsProtected() {
        GlobalBudget budget = new GlobalBudget(1000);
        budget.register("A", 500, new FakeCache());
        budget.register("B", 500, new FakeCache());

        assertThat(budget.acquire("A", 500)).isTrue();
        assertThat(budget.acquire("B", 500)).isTrue();
        assertThat(budget.totalUsage()).isEqualTo(1000);

        // Pool full, both at reserve. A can only grow by recycling its OWN LRU (B is protected), so the ceiling
        // holds and B is untouched.
        assertThat(budget.acquire("A", 200)).isTrue();
        assertThat(budget.totalUsage()).isEqualTo(1000);
        assertThat(budget.usage("A")).isEqualTo(500);   // recycled its own 200; no net growth
        assertThat(budget.usage("B")).isEqualTo(500);   // B untouched
    }

    @Test
    void blockLargerThanCapacityIsRejected() {
        GlobalBudget budget = new GlobalBudget(1000);
        budget.register("A", 1000, new FakeCache());
        assertThat(budget.acquire("A", 1001)).isFalse();
    }

    @Test
    void releaseFreesSpaceForOthers() {
        GlobalBudget budget = new GlobalBudget(1000);
        // A's usage is all within its reserve (so B cannot reclaim it); B has no reserve.
        budget.register("A", 1000, new FakeCache());
        budget.register("B", 0, new FakeCache());

        assertThat(budget.acquire("A", 1000)).isTrue();
        assertThat(budget.acquire("B", 100)).isFalse(); // full, A fully within reserve, B cannot reclaim it
        budget.release("A", 300);
        assertThat(budget.acquire("B", 100)).isTrue();
        assertThat(budget.usage("A")).isEqualTo(700);
        assertThat(budget.usage("B")).isEqualTo(100);
    }
}
