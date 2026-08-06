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

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.PolicyName;

class DecisionTreePolicySelectorTest {

    @Test
    void loadsBundledModelWithChecksum() {
        DecisionTreePolicySelector model = DecisionTreePolicySelector.fromDefaultResource();
        assertThat(model.checksum()).isNotBlank();
        // sanity: a full feature vector produces a known policy
        PolicyName p = model.predictLabel(new double[FeatureSchema.FEATURE_COUNT]);
        assertThat(p).isNotNull();
    }

    @Test
    void rejectsWrongFeatureCount() {
        DecisionTreePolicySelector model = DecisionTreePolicySelector.fromDefaultResource();
        assertThatThrownBy(() -> model.predictLabel(new double[3]))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsFeatureSchemaMismatch() {
        String badJson = "{"
                         + "\"feature_names\":[\"wrong\"],"
                         + "\"window\":64,"
                         + "\"feature_constants\":{\"small_read_bytes\":16384,\"large_read_bytes\":131072,"
                         + "\"seq_gap_bytes\":65536,\"feature_page_size\":262144},"
                         + "\"labels\":[\"s3a_random\"],"
                         + "\"nodes\":[]"
                         + "}";
        assertThatThrownBy(() -> DecisionTreePolicySelector.fromInputStream(stream(badJson)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("feature schema mismatch");
    }

    @Test
    void rejectsConstantMismatch() {
        String badJson = "{"
                         + "\"feature_names\":" + featureNamesJson() + ","
                         + "\"window\":64,"
                         + "\"feature_constants\":{\"small_read_bytes\":1,\"large_read_bytes\":131072,"
                         + "\"seq_gap_bytes\":65536,\"feature_page_size\":262144},"
                         + "\"labels\":[\"s3a_random\"],"
                         + "\"nodes\":[]"
                         + "}";
        assertThatThrownBy(() -> DecisionTreePolicySelector.fromInputStream(stream(badJson)))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("feature constant mismatch");
    }

    @Test
    void traversesSimpleHandBuiltTree() {
        // root splits on feature 0 <= 5.0 : left leaf s3a_random, right leaf s3a_prefetch
        String json = "{"
                      + "\"feature_names\":" + featureNamesJson() + ","
                      + "\"window\":64,"
                      + "\"feature_constants\":{\"small_read_bytes\":16384,\"large_read_bytes\":131072,"
                      + "\"seq_gap_bytes\":65536,\"feature_page_size\":262144},"
                      + "\"labels\":[\"s3a_random\",\"s3a_prefetch\"],"
                      + "\"nodes\":["
                      + "{\"id\":0,\"leaf\":false,\"feature_index\":0,\"threshold\":5.0,\"left\":1,\"right\":2},"
                      + "{\"id\":1,\"leaf\":true,\"predicted_label\":\"s3a_random\"},"
                      + "{\"id\":2,\"leaf\":true,\"predicted_label\":\"s3a_prefetch\"}"
                      + "]}";
        DecisionTreePolicySelector model = DecisionTreePolicySelector.fromInputStream(stream(json));

        double[] left = new double[FeatureSchema.FEATURE_COUNT];
        left[0] = 5.0; // <= 5.0 -> left
        assertThat(model.predictLabel(left)).isEqualTo(PolicyName.S3A_RANDOM);

        double[] right = new double[FeatureSchema.FEATURE_COUNT];
        right[0] = 5.0001; // > 5.0 -> right
        assertThat(model.predictLabel(right)).isEqualTo(PolicyName.S3A_PREFETCH);
    }

    private static String featureNamesJson() {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < FeatureSchema.FEATURE_NAMES.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"').append(FeatureSchema.FEATURE_NAMES.get(i)).append('"');
        }
        return sb.append(']').toString();
    }

    private static ByteArrayInputStream stream(String s) {
        return new ByteArrayInputStream(s.getBytes(StandardCharsets.UTF_8));
    }
}
