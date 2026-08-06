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

import java.util.HashMap;
import java.util.Map;
import software.amazon.awssdk.annotations.SdkPublicApi;

/**
 * The set of IO access policies the Track 1 selector can choose between. These mirror the sub-policy labels the offline
 * decision tree was trained to emit (PROJECT2 6.2). This module only <i>selects</i> a policy; the executors that turn a
 * policy into actual S3 range GETs are implemented in later stages (S2/S3).
 */
@SdkPublicApi
public enum PolicyName {

    /** Block-aligned demand read plus bounded async prefetch. Best when the working set fits the budget. */
    S3A_PREFETCH("s3a_prefetch"),

    /** Precise / small-window range GET. Best for large scans with backward seeks (suppresses over-read). */
    S3A_RANDOM("s3a_random"),

    /** Detect revisits, widen to a page and cache. Best for repeated-access (multi-epoch / locality) workloads. */
    TEMPLATE_LOCALITY("template_locality"),

    /** Two-phase: page-align small requests, demand-read large ones. Best for heterogeneous (multimodal) reads. */
    TEMPLATE_MULTIMODAL("template_multimodal");

    private static final Map<String, PolicyName> BY_LABEL = new HashMap<>();

    static {
        for (PolicyName policy : values()) {
            BY_LABEL.put(policy.label, policy);
        }
    }

    private final String label;

    PolicyName(String label) {
        this.label = label;
    }

    /**
     * The model label (as used in the exported tree JSON and the Python training pipeline).
     */
    public String label() {
        return label;
    }

    /**
     * Resolve a model label to a {@link PolicyName}, or {@code null} if it is not a known policy.
     */
    public static PolicyName fromLabel(String label) {
        return BY_LABEL.get(label);
    }
}
