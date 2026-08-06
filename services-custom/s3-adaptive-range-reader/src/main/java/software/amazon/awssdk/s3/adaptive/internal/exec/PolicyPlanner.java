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
 * Pure policy logic: given a read {@code [position, position+length)}, decide the demand range to fetch and any
 * speculative prefetch ranges. Holds no IO and no cache; it is the single source of policy behaviour shared by the
 * synchronous S2 executors and the asynchronous S3 reader. Ports the sub-policy shapes from
 * {@code prefetch_simulator.py}. Instances may be stateful (e.g. locality page-visit counts) and are single-threaded
 * (one per stream/reader).
 */
@SdkInternalApi
public interface PolicyPlanner {

    PolicyName policy();

    /**
     * Compute the demand + prefetch ranges for a read. Called exactly once per read (mutates any internal state once).
     */
    FetchPlan plan(long position, int length);

    static long alignDown(long value, long alignment) {
        return value - Math.floorMod(value, alignment);
    }

    static long alignUp(long value, long alignment) {
        return alignDown(value + alignment - 1, alignment);
    }
}
