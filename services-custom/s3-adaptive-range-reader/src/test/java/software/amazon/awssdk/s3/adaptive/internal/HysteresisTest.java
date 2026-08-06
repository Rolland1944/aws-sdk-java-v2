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

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.PolicyName;

class HysteresisTest {

    @Test
    void firstPredictionBecomesCurrentImmediately() {
        Hysteresis h = new Hysteresis(3);
        assertThat(h.current()).isNull();
        assertThat(h.apply(PolicyName.S3A_RANDOM)).isEqualTo(PolicyName.S3A_RANDOM);
        assertThat(h.current()).isEqualTo(PolicyName.S3A_RANDOM);
    }

    @Test
    void switchesOnlyAfterThreeConsecutiveAgreeingPredictions() {
        Hysteresis h = new Hysteresis(3);
        h.apply(PolicyName.S3A_RANDOM);

        assertThat(h.apply(PolicyName.S3A_PREFETCH)).isEqualTo(PolicyName.S3A_RANDOM); // pending=1
        assertThat(h.apply(PolicyName.S3A_PREFETCH)).isEqualTo(PolicyName.S3A_RANDOM); // pending=2
        assertThat(h.apply(PolicyName.S3A_PREFETCH)).isEqualTo(PolicyName.S3A_PREFETCH); // pending=3 -> switch
    }

    @Test
    void interruptedPendingResets() {
        Hysteresis h = new Hysteresis(3);
        h.apply(PolicyName.S3A_RANDOM);

        h.apply(PolicyName.S3A_PREFETCH); // pending prefetch = 1
        h.apply(PolicyName.S3A_PREFETCH); // pending prefetch = 2
        assertThat(h.apply(PolicyName.TEMPLATE_LOCALITY)).isEqualTo(PolicyName.S3A_RANDOM); // pending switches to locality=1
        assertThat(h.apply(PolicyName.TEMPLATE_LOCALITY)).isEqualTo(PolicyName.S3A_RANDOM); // locality=2
        assertThat(h.apply(PolicyName.TEMPLATE_LOCALITY)).isEqualTo(PolicyName.TEMPLATE_LOCALITY); // locality=3 -> switch
    }

    @Test
    void agreementWithCurrentClearsPending() {
        Hysteresis h = new Hysteresis(3);
        h.apply(PolicyName.S3A_RANDOM);
        h.apply(PolicyName.S3A_PREFETCH); // pending=1
        h.apply(PolicyName.S3A_RANDOM);   // back to current -> pending cleared
        // now need a fresh run of 3 to switch
        assertThat(h.apply(PolicyName.S3A_PREFETCH)).isEqualTo(PolicyName.S3A_RANDOM);
        assertThat(h.apply(PolicyName.S3A_PREFETCH)).isEqualTo(PolicyName.S3A_RANDOM);
        assertThat(h.apply(PolicyName.S3A_PREFETCH)).isEqualTo(PolicyName.S3A_PREFETCH);
    }

    @Test
    void resetClearsState() {
        Hysteresis h = new Hysteresis(3);
        h.apply(PolicyName.S3A_RANDOM);
        h.reset();
        assertThat(h.current()).isNull();
    }
}
