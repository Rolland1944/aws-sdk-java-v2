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

package software.amazon.awssdk.s3.adaptive.internal.exec;

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.PolicyName;

/**
 * {@code s3a_random}: demand read with a small (64 KiB) readahead and no speculative prefetch (scattered/backward
 * access must not over-read). Ports {@code S3AFadviseRandomPolicy.on_read}.
 */
@SdkInternalApi
public final class RandomPlanner implements PolicyPlanner {

    private static final long READAHEAD = 64L * 1024;

    @Override
    public PolicyName policy() {
        return PolicyName.S3A_RANDOM;
    }

    @Override
    public FetchPlan plan(long position, int length) {
        return new FetchPlan(new FetchRange(position, position + Math.max(length, READAHEAD)));
    }
}
