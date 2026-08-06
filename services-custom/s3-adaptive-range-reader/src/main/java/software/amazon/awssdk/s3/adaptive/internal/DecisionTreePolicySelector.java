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

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.protocols.jsoncore.JsonNode;
import software.amazon.awssdk.s3.adaptive.PolicyName;

/**
 * Dependency-free inferencer for the offline-trained Track 1 decision tree (PROJECT2 §7.2). Loads the versioned tree
 * JSON exported by {@code tools/export_policy_tree.py}, validates that its feature schema matches {@link FeatureSchema}
 * (so training and inference cannot drift), and evaluates the tree.
 *
 * <p>Split semantics match sklearn: at an internal node go LEFT iff {@code features[featureIndex] <= threshold}, else
 * RIGHT; a leaf yields the label with the highest class weight. This class is immutable and thread-safe; per-stream
 * online state (window + hysteresis) lives in {@link AdaptivePolicySelector}.
 */
@SdkInternalApi
public final class DecisionTreePolicySelector {

    private static final String DEFAULT_RESOURCE = "/software/amazon/awssdk/s3/adaptive/policy_selector_v1.json";

    private final int[] featureIndex;
    private final double[] threshold;
    private final int[] left;
    private final int[] right;
    private final PolicyName[] leafLabel;
    private final String checksum;

    private DecisionTreePolicySelector(int[] featureIndex, double[] threshold, int[] left, int[] right,
                                       PolicyName[] leafLabel, String checksum) {
        this.featureIndex = featureIndex;
        this.threshold = threshold;
        this.left = left;
        this.right = right;
        this.leafLabel = leafLabel;
        this.checksum = checksum;
    }

    /**
     * Load the tree bundled with this module.
     */
    public static DecisionTreePolicySelector fromDefaultResource() {
        try (InputStream in = DecisionTreePolicySelector.class.getResourceAsStream(DEFAULT_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("bundled model resource not found: " + DEFAULT_RESOURCE);
            }
            return fromInputStream(in);
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("failed to load bundled policy model", e);
        }
    }

    /**
     * Load and validate a tree from a JSON stream.
     */
    public static DecisionTreePolicySelector fromInputStream(InputStream in) {
        JsonNode root = JsonNode.parser().parse(in);
        Map<String, JsonNode> obj = root.asObject();

        validateSchema(obj);

        List<JsonNode> nodes = requiredField(obj, "nodes").asArray();
        int count = nodes.size();
        int[] featureIndex = new int[count];
        double[] threshold = new double[count];
        int[] left = new int[count];
        int[] right = new int[count];
        PolicyName[] leafLabel = new PolicyName[count];

        for (JsonNode nodeNode : nodes) {
            Map<String, JsonNode> node = nodeNode.asObject();
            int id = intField(node, "id");
            if (id < 0 || id >= count) {
                throw new IllegalStateException("node id out of range: " + id);
            }
            boolean leaf = requiredField(node, "leaf").asBoolean();
            if (leaf) {
                String label = requiredField(node, "predicted_label").asString();
                PolicyName policy = PolicyName.fromLabel(label);
                if (policy == null) {
                    throw new IllegalStateException("unknown leaf label: " + label);
                }
                leafLabel[id] = policy;
                featureIndex[id] = -1;
                left[id] = -1;
                right[id] = -1;
            } else {
                featureIndex[id] = intField(node, "feature_index");
                threshold[id] = doubleField(node, "threshold");
                left[id] = intField(node, "left");
                right[id] = intField(node, "right");
                if (featureIndex[id] < 0 || featureIndex[id] >= FeatureSchema.FEATURE_COUNT) {
                    throw new IllegalStateException("feature_index out of range at node " + id);
                }
            }
        }

        String checksum = obj.containsKey("sha256_checksum") ? obj.get("sha256_checksum").asString() : null;
        return new DecisionTreePolicySelector(featureIndex, threshold, left, right, leafLabel, checksum);
    }

    /**
     * Evaluate the tree for a feature vector and return the predicted policy.
     */
    public PolicyName predictLabel(double[] features) {
        if (features.length != FeatureSchema.FEATURE_COUNT) {
            throw new IllegalArgumentException("expected " + FeatureSchema.FEATURE_COUNT
                                               + " features but got " + features.length);
        }
        int node = 0;
        while (leafLabel[node] == null) {
            node = features[featureIndex[node]] <= threshold[node] ? left[node] : right[node];
        }
        return leafLabel[node];
    }

    /**
     * The sha256 checksum recorded in the exported model, for auditing.
     */
    public String checksum() {
        return checksum;
    }

    private static void validateSchema(Map<String, JsonNode> obj) {
        List<JsonNode> names = requiredField(obj, "feature_names").asArray();
        List<String> loaded = new ArrayList<>(names.size());
        for (JsonNode n : names) {
            loaded.add(n.asString());
        }
        if (!loaded.equals(FeatureSchema.FEATURE_NAMES)) {
            throw new IllegalStateException("feature schema mismatch:\n  model:  " + loaded
                                            + "\n  reader: " + FeatureSchema.FEATURE_NAMES);
        }

        int window = intField(obj, "window");
        if (window != FeatureSchema.WINDOW) {
            throw new IllegalStateException("window mismatch: model=" + window + " reader=" + FeatureSchema.WINDOW);
        }

        Map<String, JsonNode> constants = requiredField(obj, "feature_constants").asObject();
        checkConstant(constants, "small_read_bytes", FeatureSchema.SMALL_READ_BYTES);
        checkConstant(constants, "large_read_bytes", FeatureSchema.LARGE_READ_BYTES);
        checkConstant(constants, "seq_gap_bytes", FeatureSchema.SEQ_GAP_BYTES);
        checkConstant(constants, "feature_page_size", FeatureSchema.FEATURE_PAGE_SIZE);

        List<JsonNode> labels = requiredField(obj, "labels").asArray();
        for (JsonNode label : labels) {
            String value = label.asString();
            if (PolicyName.fromLabel(value) == null) {
                throw new IllegalStateException("model declares unknown label: " + value);
            }
        }
    }

    private static void checkConstant(Map<String, JsonNode> constants, String key, long expected) {
        long actual = Long.parseLong(requiredField(constants, key).asNumber());
        if (actual != expected) {
            throw new IllegalStateException("feature constant mismatch for " + key
                                            + ": model=" + actual + " reader=" + expected);
        }
    }

    private static JsonNode requiredField(Map<String, JsonNode> obj, String key) {
        JsonNode node = obj.get(key);
        if (node == null) {
            throw new IllegalStateException("missing required field: " + key);
        }
        return node;
    }

    private static int intField(Map<String, JsonNode> obj, String key) {
        return Integer.parseInt(requiredField(obj, key).asNumber());
    }

    private static double doubleField(Map<String, JsonNode> obj, String key) {
        return Double.parseDouble(requiredField(obj, key).asNumber());
    }
}
