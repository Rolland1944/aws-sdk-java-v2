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
 * {@code template_multimodal}: small reads ({@code <= 16 KiB}) are page-aligned to 64 KiB and coalesced; large reads
 * use an exact demand range. No speculative prefetch. Ports {@code TemplateMultimodalTwoPhasePolicy.on_read}.
 */
@SdkInternalApi
public final class MultimodalPlanner implements PolicyPlanner {

    private static final long SMALL_THRESHOLD = 16L * 1024;
    private static final long SMALL_PAGE = 64L * 1024;

    @Override
    public PolicyName policy() {
        return PolicyName.TEMPLATE_MULTIMODAL;
    }

    @Override
    public FetchPlan plan(long position, int length) {
        if (length <= SMALL_THRESHOLD) {
            long start = PolicyPlanner.alignDown(position, SMALL_PAGE);
            long end = PolicyPlanner.alignUp(position + length, SMALL_PAGE);
            return new FetchPlan(new FetchRange(start, end));
        }
        return new FetchPlan(new FetchRange(position, position + length));
    }
}
