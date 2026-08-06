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

package software.amazon.awssdk.s3.adaptive;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.internal.AdaptivePolicySelector;
import software.amazon.awssdk.s3.adaptive.internal.DecisionTreePolicySelector;
import software.amazon.awssdk.s3.adaptive.internal.IoRequest;

/**
 * Lightweight microbenchmark (not JMH) asserting that one online step -- feature update over the 64-read window plus a
 * tree traversal -- stays in the microsecond range, so the selector is cheap enough to sit on the read path
 * (PROJECT2 §7.3 S1 step 4). The threshold is deliberately generous to avoid CI flakiness; it is a gross-regression
 * guard, and the measured ns/op is printed for reference.
 */
class InferenceMicroBenchmarkTest {

    private static final int WARMUP = 100_000;
    private static final int MEASURE = 500_000;
    // Generous upper bound: a warm decision-tree step is typically well under 1us; 50us catches gross regressions
    // without being flaky on shared/slow CI hardware.
    private static final double MAX_NS_PER_OP = 50_000.0;

    @Test
    void singleStepIsMicrosecondScale() {
        DecisionTreePolicySelector model = DecisionTreePolicySelector.fromDefaultResource();
        AdaptivePolicySelector selector = new AdaptivePolicySelector(model);
        List<IoRequest> stream = syntheticStream();

        long sink = 0;
        for (int i = 0; i < WARMUP; i++) {
            sink += selector.onRead(stream.get(i % stream.size())).ordinal();
        }

        long start = System.nanoTime();
        for (int i = 0; i < MEASURE; i++) {
            sink += selector.onRead(stream.get(i % stream.size())).ordinal();
        }
        long elapsed = System.nanoTime() - start;

        double nsPerOp = (double) elapsed / MEASURE;
        System.out.printf("[microbench] onRead: %.1f ns/op over %d ops (sink=%d)%n", nsPerOp, MEASURE, sink);

        assertThat(nsPerOp).isLessThan(MAX_NS_PER_OP);
    }

    private static List<IoRequest> syntheticStream() {
        List<IoRequest> stream = new ArrayList<>();
        // A mix that exercises different branches: small random, large sequential, revisits.
        for (int i = 0; i < 32; i++) {
            stream.add(new IoRequest("obj/small", (long) i * 512, 384, 1_000_000.0));
        }
        long seqBase = 0;
        for (int i = 0; i < 16; i++) {
            stream.add(new IoRequest("obj/scan", seqBase, 512 * 1024, 500_000_000.0));
            seqBase += 512 * 1024;
        }
        for (int i = 0; i < 16; i++) {
            stream.add(new IoRequest("obj/page", (long) (i % 4) * 256 * 1024, 2048, 8_000_000.0));
        }
        return stream;
    }
}
