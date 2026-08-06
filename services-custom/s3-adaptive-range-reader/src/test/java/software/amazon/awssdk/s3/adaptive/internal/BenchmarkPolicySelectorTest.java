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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.PolicyName;

class BenchmarkPolicySelectorTest {

    @Test
    void fixedSelectorAlwaysReturnsConfiguredPolicy() {
        FixedPolicySelector selector = new FixedPolicySelector(PolicyName.S3A_RANDOM);

        assertThat(selector.onRead(request("tpch/lineitem.parquet"))).isEqualTo(PolicyName.S3A_RANDOM);
        assertThat(selector.onRead(request("mm/retrieval.bin"))).isEqualTo(PolicyName.S3A_RANDOM);
        assertThat(selector.currentPolicy()).isEqualTo(PolicyName.S3A_RANDOM);
    }

    @Test
    void prefixSelectorUsesLongestMatchingPrefix() {
        Map<String, PolicyName> policies = new LinkedHashMap<>();
        policies.put("bench/tpch/", PolicyName.S3A_RANDOM);
        policies.put("bench/tpch/hot/", PolicyName.TEMPLATE_LOCALITY);
        PrefixPolicySelector selector = new PrefixPolicySelector(policies);

        assertThat(selector.onRead(request("bench/tpch/lineitem.parquet"))).isEqualTo(PolicyName.S3A_RANDOM);
        assertThat(selector.onRead(request("bench/tpch/hot/cache.parquet"))).isEqualTo(PolicyName.TEMPLATE_LOCALITY);
        assertThat(selector.currentPolicy()).isEqualTo(PolicyName.TEMPLATE_LOCALITY);
    }

    @Test
    void prefixSelectorRejectsUnmappedKey() {
        PrefixPolicySelector selector = new PrefixPolicySelector(
            java.util.Collections.singletonMap("bench/tpch/", PolicyName.S3A_RANDOM));

        assertThatThrownBy(() -> selector.onRead(request("bench/mm/retrieval.bin")))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("No oracle policy");
    }

    private static IoRequest request(String key) {
        return new IoRequest(key, 0, 1, 1.0);
    }
}
