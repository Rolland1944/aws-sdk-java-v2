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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class RangeMergerTest {

    @Test
    void mergesAcrossSmallGap() {
        List<RangeMerger.Group> groups = RangeMerger.group(
            Arrays.asList(span(0, 9), span(20, 29)), 16, 1024, 0.5);
        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).start).isEqualTo(0);
        assertThat(groups.get(0).endInclusive).isEqualTo(29);
        assertThat(groups.get(0).wasteBytes).isEqualTo(10);
        assertThat(groups.get(0).members).containsExactly(0, 1);
    }

    @Test
    void splitsWhenGapExceedsGStar() {
        List<RangeMerger.Group> groups = RangeMerger.group(
            Arrays.asList(span(0, 9), span(100, 109)), 16, 1024, 0.9);
        assertThat(groups).hasSize(2);
    }

    @Test
    void splitsWhenWasteRatioTooHigh() {
        List<RangeMerger.Group> groups = RangeMerger.group(
            Arrays.asList(span(0, 9), span(20, 29)), 16, 1024, 0.1);
        assertThat(groups).hasSize(2);
    }

    @Test
    void splitsWhenMergedExceedsMaxBytes() {
        List<RangeMerger.Group> groups = RangeMerger.group(
            Arrays.asList(span(0, 9), span(10, 19)), 16, 15, 1.0);
        assertThat(groups).hasSize(2);
    }

    @Test
    void overlappingRangesHaveZeroWaste() {
        List<RangeMerger.Group> groups = RangeMerger.group(
            Arrays.asList(span(0, 20), span(10, 30)), 0, 1024, 0.0);
        assertThat(groups).hasSize(1);
        assertThat(groups.get(0).usefulBytes).isEqualTo(31);
        assertThat(groups.get(0).wasteBytes).isEqualTo(0);
    }

    private static RangeMerger.Span span(long start, long end) {
        return new RangeMerger.Span(start, end);
    }
}
