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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Collections;
import org.junit.jupiter.api.Test;

class AdaptiveReaderRuntimeBuilderTest {

    @Test
    void rejectsFixedAndPrefixSelectorsTogether() {
        assertThatThrownBy(() -> AdaptiveReaderRuntime.builder()
                                                       .forcePolicy(PolicyName.S3A_RANDOM)
                                                       .prefixPolicyMap(Collections.singletonMap(
                                                           "bench/tpch/", PolicyName.S3A_PREFETCH))
                                                       .build())
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("cannot both be configured");
    }
}
