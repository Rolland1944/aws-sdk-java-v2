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

/**
 * A half-open byte range {@code [start, endExclusive)} a policy would like to fetch. The abstract executor clamps it
 * to cover the actual request and to the object bounds, so a policy only expresses its preferred shape.
 */
@SdkInternalApi
public final class FetchRange {

    private final long start;
    private final long endExclusive;

    public FetchRange(long start, long endExclusive) {
        this.start = start;
        this.endExclusive = endExclusive;
    }

    public long start() {
        return start;
    }

    public long endExclusive() {
        return endExclusive;
    }
}
