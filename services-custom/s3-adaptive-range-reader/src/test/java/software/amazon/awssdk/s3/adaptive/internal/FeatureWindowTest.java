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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

import org.junit.jupiter.api.Test;

class FeatureWindowTest {

    private static final double TOL = 1e-12;
    private static final long KB = 1024;

    private static double log2(double v) {
        return Math.log(v) / Math.log(2.0);
    }

    @Test
    void emptyHistory_singleRead_usesDefaults() {
        FeatureWindow window = new FeatureWindow();
        double[] f = window.featuresFor(new IoRequest("o", 0, 4 * KB, null));

        assertThat(f[0]).isCloseTo(log2(4 * KB), offset(TOL)); // log2_cur_size
        assertThat(f[1]).isCloseTo(log2(4 * KB), offset(TOL)); // log2_med_size (median of one)
        assertThat(f[2]).isEqualTo(1.0);   // frac_small (4KB <= 16KB)
        assertThat(f[3]).isEqualTo(0.0);   // frac_large
        assertThat(f[4]).isEqualTo(0.0);   // sequentiality: no pairs
        assertThat(f[5]).isEqualTo(1.0);   // forward_ratio: default when no pairs
        assertThat(f[6]).isEqualTo(0.0);   // log2_med_gap = log2(1+0)
        assertThat(f[7]).isEqualTo(0.0);   // page_revisit
        assertThat(f[8]).isEqualTo(1.0);   // distinct_obj
        assertThat(f[9]).isEqualTo(0.0);   // size_ratio: null file size
    }

    @Test
    void medianOfSizes_evenAndOdd() {
        FeatureWindow window = new FeatureWindow();
        window.add(new IoRequest("o", 0, 10, null));
        window.add(new IoRequest("o", 10, 20, null));
        // odd window: sizes {10,20,30} -> median 20
        double[] odd = window.featuresFor(new IoRequest("o", 30, 30, null));
        assertThat(odd[1]).isCloseTo(log2(20.0), offset(TOL));

        window.add(new IoRequest("o", 30, 30, null));
        // even window: sizes {10,20,30,40} -> median (20+30)/2 = 25
        double[] even = window.featuresFor(new IoRequest("o", 60, 40, null));
        assertThat(even[1]).isCloseTo(log2(25.0), offset(TOL));
    }

    @Test
    void sizeBoundaries_areInclusive() {
        FeatureWindow window = new FeatureWindow();
        double[] small = window.featuresFor(new IoRequest("o", 0, 16 * KB, null)); // == SMALL_READ_BYTES
        assertThat(small[2]).isEqualTo(1.0); // frac_small inclusive of 16KB
        assertThat(small[3]).isEqualTo(0.0);

        double[] large = new FeatureWindow().featuresFor(new IoRequest("o", 0, 128 * KB, null)); // == LARGE_READ_BYTES
        assertThat(large[2]).isEqualTo(0.0);
        assertThat(large[3]).isEqualTo(1.0); // frac_large inclusive of 128KB
    }

    @Test
    void fileSize_invalidValuesYieldZeroRatio_validClampsToOne() {
        assertThat(new FeatureWindow().featuresFor(new IoRequest("o", 0, 8 * KB, Double.NaN))[9]).isEqualTo(0.0);
        assertThat(new FeatureWindow().featuresFor(new IoRequest("o", 0, 8 * KB, 0.0))[9]).isEqualTo(0.0);
        assertThat(new FeatureWindow().featuresFor(new IoRequest("o", 0, 8 * KB, -5.0))[9]).isEqualTo(0.0);
        // length larger than file size clamps to 1.0
        assertThat(new FeatureWindow().featuresFor(new IoRequest("o", 0, 10 * KB, 4.0 * KB))[9]).isEqualTo(1.0);
        // normal ratio
        assertThat(new FeatureWindow().featuresFor(new IoRequest("o", 0, KB, 4.0 * KB))[9])
            .isCloseTo(0.25, offset(TOL));
    }

    @Test
    void sequentiality_forwardRatio_andGap() {
        FeatureWindow window = new FeatureWindow();
        window.add(new IoRequest("o", 0, 1000, null));
        // next read starts exactly at previous end -> gap 0 -> sequential, forward
        double[] f = window.featuresFor(new IoRequest("o", 1000, 1000, null));
        assertThat(f[4]).isEqualTo(1.0); // sequentiality
        assertThat(f[5]).isEqualTo(1.0); // forward_ratio
        assertThat(f[6]).isCloseTo(log2(1.0), offset(TOL)); // gap 0 -> log2(1+0)=0

        // backward jump breaks forward ratio and sequentiality
        FeatureWindow w2 = new FeatureWindow();
        w2.add(new IoRequest("o", 5000, 1000, null));
        double[] b = w2.featuresFor(new IoRequest("o", 0, 1000, null));
        assertThat(b[4]).isEqualTo(0.0); // not sequential (negative gap)
        assertThat(b[5]).isEqualTo(0.0); // not forward
    }

    @Test
    void pageRevisit_countedPerObjectAt256Kib() {
        FeatureWindow window = new FeatureWindow();
        long page = FeatureSchema.FEATURE_PAGE_SIZE;
        window.add(new IoRequest("o", page * 2 + 10, 512, null));
        // second read into the same 256KiB page -> one revisit over window of size 2
        double[] f = window.featuresFor(new IoRequest("o", page * 2 + 2000, 512, null));
        assertThat(f[7]).isCloseTo(0.5, offset(TOL));
    }

    @Test
    void distinctObjectRatio() {
        FeatureWindow window = new FeatureWindow();
        window.add(new IoRequest("a", 0, 100, null));
        window.add(new IoRequest("b", 0, 100, null));
        double[] f = window.featuresFor(new IoRequest("c", 0, 100, null));
        assertThat(f[8]).isCloseTo(1.0, offset(TOL)); // 3 distinct / 3
    }

    @Test
    void windowIsBounded() {
        FeatureWindow window = new FeatureWindow(4);
        for (int i = 0; i < 10; i++) {
            window.add(new IoRequest("o", i * 100, 100, null));
        }
        assertThat(window.size()).isEqualTo(4);
    }
}
