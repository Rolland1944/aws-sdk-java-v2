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

package software.amazon.awssdk.s3.adaptive.s3a;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class WorkingSetWindowTest {

    @Test
    void firstVisitIsDistinctNotReusable() {
        WorkingSetWindow w = new WorkingSetWindow(16);
        w.observe("o", 0, 100);
        w.observe("o", 200, 50);
        assertThat(w.distinctBytes()).isEqualTo(150);
        assertThat(w.reusableBytes()).isEqualTo(0);
        assertThat(w.seenBefore("o", 0, 100)).isTrue();
        assertThat(w.seenBefore("o", 400, 10)).isFalse();
    }

    @Test
    void secondVisitPromotesToReusable() {
        WorkingSetWindow w = new WorkingSetWindow(16);
        w.observe("o", 0, 100);
        w.observe("o", 0, 100);
        assertThat(w.distinctBytes()).isEqualTo(100);
        assertThat(w.reusableBytes()).isEqualTo(100);
        assertThat(w.reusedSizePercentile(0.90)).isEqualTo(100);
    }

    @Test
    void evictionDropsDistinctThenReusable() {
        WorkingSetWindow w = new WorkingSetWindow(16);
        w.observe("a", 0, 10);
        for (int i = 0; i < 20; i++) {
            w.observe("b", i * 100L, 8);
        }
        assertThat(w.size()).isEqualTo(16);
        assertThat(w.seenBefore("a", 0, 10)).isFalse();
    }

    @Test
    void byteHorizonEvictsOldestEvenWhenEventCapIsLarge() {
        WorkingSetWindow w = new WorkingSetWindow(100, 1000);
        w.observe("o", 0, 40);
        w.observe("o", 40, 40);
        w.observe("o", 80, 40);
        assertThat(w.size()).isEqualTo(2);
        assertThat(w.eventBytes()).isEqualTo(80);
        assertThat(w.seenBefore("o", 0, 40)).isFalse();
        assertThat(w.seenBefore("o", 80, 40)).isTrue();
    }

    @Test
    void pollutionTracksOversizeShare() {
        WorkingSetWindow w = new WorkingSetWindow(Long.MAX_VALUE, 32, 256 * 1024);
        w.observe("hot", 0, 128 * 1024);
        w.observe("scan", 0, 4 * 1024 * 1024);
        assertThat(w.pollution()).isGreaterThan(0.9);
        w.reset();
        assertThat(w.eventBytes()).isEqualTo(0);
        assertThat(w.pollution()).isEqualTo(0);
    }
}
