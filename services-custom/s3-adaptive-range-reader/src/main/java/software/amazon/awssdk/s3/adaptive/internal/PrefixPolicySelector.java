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

import java.util.LinkedHashMap;
import java.util.Map;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.PolicyName;

/**
 * Selects a fixed policy from the longest matching object-key prefix. It is a
 * benchmark oracle: a perfect workload classifier plus a measured
 * prefix-to-policy table can realize the same routing in a real shared cache.
 */
@SdkInternalApi
public final class PrefixPolicySelector implements SharedPolicySelector {

    private final Map<String, PolicyName> policies;
    private PolicyName current;

    public PrefixPolicySelector(Map<String, PolicyName> policies) {
        if (policies == null || policies.isEmpty()) {
            throw new IllegalArgumentException("policies must not be empty");
        }
        this.policies = new LinkedHashMap<>(policies);
    }

    @Override
    public synchronized PolicyName onRead(IoRequest req) {
        PolicyName selected = null;
        int longest = -1;
        for (Map.Entry<String, PolicyName> entry : policies.entrySet()) {
            if (req.objectKey().startsWith(entry.getKey()) && entry.getKey().length() > longest) {
                selected = entry.getValue();
                longest = entry.getKey().length();
            }
        }
        if (selected == null) {
            throw new IllegalArgumentException("No oracle policy for object key: " + req.objectKey());
        }
        current = selected;
        return selected;
    }

    @Override
    public synchronized PolicyName currentPolicy() {
        return current;
    }
}
