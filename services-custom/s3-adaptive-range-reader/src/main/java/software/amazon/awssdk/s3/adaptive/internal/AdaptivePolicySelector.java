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

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.PolicyName;

/**
 * The online "policy brain" for a single logical read stream: it ties together the {@link FeatureWindow}, the
 * immutable {@link DecisionTreePolicySelector}, and {@link Hysteresis}. On each read it derives features from recent
 * history, asks the tree which policy fits, smooths the decision, and reports the current policy.
 *
 * <p>This is a 1:1 behavioural port of {@code prefetch_simulator.StatisticalPolicySelector.on_read}, minus the
 * quantized prediction cache (which is a Python-side speed optimization, not part of the decision logic). It performs
 * no S3 IO; executing a chosen policy is the job of later stages (S2/S3).
 *
 * <p>Not thread-safe: create one per stream. The underlying {@link DecisionTreePolicySelector} is immutable and can be
 * shared across many instances.
 */
@SdkInternalApi
public final class AdaptivePolicySelector {

    private final DecisionTreePolicySelector model;
    private final FeatureWindow window;
    private final Hysteresis hysteresis;

    public AdaptivePolicySelector(DecisionTreePolicySelector model) {
        this(model, new FeatureWindow(), new Hysteresis());
    }

    public AdaptivePolicySelector(DecisionTreePolicySelector model, FeatureWindow window, Hysteresis hysteresis) {
        this.model = model;
        this.window = window;
        this.hysteresis = hysteresis;
    }

    /**
     * Create a selector backed by the model bundled with this module.
     */
    public static AdaptivePolicySelector create() {
        return new AdaptivePolicySelector(DecisionTreePolicySelector.fromDefaultResource());
    }

    /**
     * Observe a read and return the policy to use for it (after hysteresis smoothing).
     */
    public PolicyName onRead(IoRequest req) {
        double[] features = window.featuresFor(req);
        PolicyName label = model.predictLabel(features);
        PolicyName current = hysteresis.apply(label);
        window.add(req);
        return current;
    }

    /**
     * The current policy, or {@code null} if no read has been observed yet.
     */
    public PolicyName currentPolicy() {
        return hysteresis.current();
    }

    /**
     * Reset all per-stream state (window + hysteresis) to start a fresh stream.
     */
    public void reset() {
        window.reset();
        hysteresis.reset();
    }
}
