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
 * A selector that always returns one policy. Used to measure a static-policy
 * baseline under the same runtime, cache, and executor configuration as an
 * adaptive selector.
 */
@SdkInternalApi
public final class FixedPolicySelector implements SharedPolicySelector {

    private final PolicyName policy;

    public FixedPolicySelector(PolicyName policy) {
        if (policy == null) {
            throw new IllegalArgumentException("policy must not be null");
        }
        this.policy = policy;
    }

    @Override
    public PolicyName onRead(IoRequest req) {
        return policy;
    }

    @Override
    public PolicyName currentPolicy() {
        return policy;
    }
}
