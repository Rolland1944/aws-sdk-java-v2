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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.internal.RuntimeConfig;
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache.ReplacementPolicy;

class Track1S3aRuntimePolicyTest {

    private static ReplacementPolicy resolve(boolean adaptive, String policy) {
        return Track1S3aRuntime.resolvePolicy(
            RuntimeConfig.builder().d1Adaptive(adaptive).d1Policy(policy).build());
    }

    @Test
    void emptyPolicyKeepsTheLegacyDerivation() {
        assertThat(resolve(true, "")).isEqualTo(ReplacementPolicy.WTINYLFU);
        assertThat(resolve(false, "")).isEqualTo(ReplacementPolicy.LRU);
        assertThat(resolve(true, null)).isEqualTo(ReplacementPolicy.WTINYLFU);
    }

    @Test
    void explicitPolicyWinsRegardlessOfAdaptive() {
        assertThat(resolve(true, "gdsf")).isEqualTo(ReplacementPolicy.GDSF);
        assertThat(resolve(true, "GDSF-Unit")).isEqualTo(ReplacementPolicy.GDSF_UNIT);
        assertThat(resolve(true, " sieve ")).isEqualTo(ReplacementPolicy.SIEVE);
        assertThat(resolve(true, "wtinylfu-av")).isEqualTo(ReplacementPolicy.WTINYLFU_AV);
        assertThat(resolve(true, "slru")).isEqualTo(ReplacementPolicy.SLRU);
        assertThat(resolve(true, "lru")).isEqualTo(ReplacementPolicy.LRU);
        assertThat(resolve(false, "wtinylfu")).isEqualTo(ReplacementPolicy.WTINYLFU);
    }

    @Test
    void unknownPolicyFailsFast() {
        assertThatThrownBy(() -> resolve(true, "arc")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void runtimeReportsThePolicyInStats() {
        Track1S3aRuntime runtime = Track1S3aRuntime.resetShared(
            RuntimeConfig.builder().d1Enabled(true).d1Adaptive(true).d1Policy("sieve").build());
        try {
            assertThat(runtime.snapshotJson()).contains("\"d1_policy\":\"SIEVE\"");
        } finally {
            runtime.close();
        }
    }
}
