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

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.protocols.jsoncore.JsonNode;
import software.amazon.awssdk.s3.adaptive.internal.DecisionTreePolicySelector;
import software.amazon.awssdk.s3.adaptive.internal.FeatureSchema;
import software.amazon.awssdk.s3.adaptive.internal.FeatureWindow;
import software.amazon.awssdk.s3.adaptive.internal.Hysteresis;
import software.amazon.awssdk.s3.adaptive.internal.IoRequest;

/**
 * Replays the Python-generated golden vectors through the Java feature window + decision tree + hysteresis and asserts
 * that Java reproduces the Python reference: features within tolerance, predicted label and post-hysteresis current
 * policy 100% identical. This is the S1 acceptance gate (PROJECT2 §7.3).
 */
class GoldenConsistencyTest {

    private static final String GOLDEN_RESOURCE = "/golden/policy_golden_v1.json";
    // log2 is computed as Math.log(x)/Math.log(2) in Java vs numpy.log2 in Python; allow a tiny numerical slack.
    private static final double FEATURE_ABS_TOL = 1e-9;

    @Test
    void javaMatchesPythonGolden() {
        DecisionTreePolicySelector model = DecisionTreePolicySelector.fromDefaultResource();
        Map<String, JsonNode> golden = loadGolden();

        // The golden set must be for the exact model this module ships.
        String goldenChecksum = golden.containsKey("model_checksum") && !golden.get("model_checksum").isNull()
                                 ? golden.get("model_checksum").asString() : null;
        assertThat(model.checksum()).isEqualTo(goldenChecksum);
        assertThat(intOf(golden, "window")).isEqualTo(FeatureSchema.WINDOW);

        int hysteresisThreshold = intOf(golden, "hysteresis");
        List<JsonNode> segments = golden.get("segments").asArray();

        int totalReads = 0;
        int labelChecks = 0;
        int currentChecks = 0;
        for (JsonNode segmentNode : segments) {
            Map<String, JsonNode> segment = segmentNode.asObject();
            String name = segment.get("name").asString();
            List<JsonNode> reads = segment.get("reads").asArray();

            FeatureWindow window = new FeatureWindow();
            Hysteresis hysteresis = new Hysteresis(hysteresisThreshold);

            for (int i = 0; i < reads.size(); i++) {
                Map<String, JsonNode> read = reads.get(i).asObject();
                IoRequest req = new IoRequest(
                    read.get("object_key").asString(),
                    longOf(read, "offset"),
                    longOf(read, "length"),
                    nullableDouble(read, "file_size"));

                double[] expectedFeatures = doubleArray(read.get("expected_features").asArray());
                double[] actualFeatures = window.featuresFor(req);

                for (int f = 0; f < FeatureSchema.FEATURE_COUNT; f++) {
                    assertThat(actualFeatures[f])
                        .withFailMessage("feature[%s]=%s mismatch in segment %s read %s: expected %s got %s",
                                         f, FeatureSchema.FEATURE_NAMES.get(f), name, i,
                                         expectedFeatures[f], actualFeatures[f])
                        .isCloseTo(expectedFeatures[f], org.assertj.core.data.Offset.offset(FEATURE_ABS_TOL));
                }

                PolicyName label = model.predictLabel(actualFeatures);
                assertThat(label.label())
                    .withFailMessage("label mismatch in segment %s read %s: expected %s got %s",
                                     name, i, read.get("expected_label").asString(), label.label())
                    .isEqualTo(read.get("expected_label").asString());
                labelChecks++;

                PolicyName current = hysteresis.apply(label);
                assertThat(current.label())
                    .withFailMessage("current-policy mismatch in segment %s read %s: expected %s got %s",
                                     name, i, read.get("expected_current").asString(), current.label())
                    .isEqualTo(read.get("expected_current").asString());
                currentChecks++;

                window.add(req);
                totalReads++;
            }
        }

        assertThat(totalReads).isEqualTo(intOf(golden, "total_reads"));
        assertThat(totalReads).isGreaterThanOrEqualTo(1000);
        assertThat(labelChecks).isEqualTo(totalReads);
        assertThat(currentChecks).isEqualTo(totalReads);
    }

    private static Map<String, JsonNode> loadGolden() {
        try (InputStream in = GoldenConsistencyTest.class.getResourceAsStream(GOLDEN_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("golden resource not found: " + GOLDEN_RESOURCE);
            }
            return JsonNode.parser().parse(in).asObject();
        } catch (Exception e) {
            throw new IllegalStateException("failed to load golden vectors", e);
        }
    }

    private static double[] doubleArray(List<JsonNode> array) {
        double[] out = new double[array.size()];
        for (int i = 0; i < array.size(); i++) {
            out[i] = Double.parseDouble(array.get(i).asNumber());
        }
        return out;
    }

    private static int intOf(Map<String, JsonNode> obj, String key) {
        return Integer.parseInt(obj.get(key).asNumber());
    }

    private static long longOf(Map<String, JsonNode> obj, String key) {
        return Long.parseLong(obj.get(key).asNumber());
    }

    private static Double nullableDouble(Map<String, JsonNode> obj, String key) {
        JsonNode node = obj.get(key);
        if (node == null || node.isNull()) {
            return null;
        }
        return Double.parseDouble(node.asNumber());
    }
}
