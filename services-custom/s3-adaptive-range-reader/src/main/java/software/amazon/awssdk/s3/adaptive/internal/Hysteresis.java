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
 * Switch-smoothing for the per-read policy predictions: a new policy only becomes current after the model has
 * proposed it {@code k} times in a row (default 3). This is a 1:1 port of
 * {@code StatisticalPolicySelector._apply_hysteresis}, which prevents rapid flip-flapping during workload transitions.
 *
 * <p>Not thread-safe: one instance tracks a single logical read stream.
 */
@SdkInternalApi
public final class Hysteresis {

    private final int threshold;
    private PolicyName current;
    private PolicyName pending;
    private int pendingCount;

    public Hysteresis() {
        this(3);
    }

    public Hysteresis(int threshold) {
        this.threshold = threshold;
    }

    /**
     * Feed the latest raw prediction and return the (possibly unchanged) current policy.
     */
    public PolicyName apply(PolicyName label) {
        if (current == null) {
            current = label;
            return current;
        }
        if (label == current) {
            pending = null;
            pendingCount = 0;
            return current;
        }
        if (label == pending) {
            pendingCount++;
        } else {
            pending = label;
            pendingCount = 1;
        }
        if (pendingCount >= threshold) {
            current = label;
            pending = null;
            pendingCount = 0;
        }
        return current;
    }

    public PolicyName current() {
        return current;
    }

    public void reset() {
        current = null;
        pending = null;
        pendingCount = 0;
    }
}
