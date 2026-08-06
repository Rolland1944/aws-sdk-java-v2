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

import java.util.Collections;
import java.util.List;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * The output of a {@link PolicyPlanner}: the range that must be fetched to satisfy the current read ({@link #demand()})
 * plus any speculative ranges the policy would like to prefetch ahead ({@link #prefetch()}). Computed together in one
 * call so a stateful planner (e.g. locality page-visit counting) mutates its state exactly once per read.
 */
@SdkInternalApi
public final class FetchPlan {

    private final FetchRange demand;
    private final List<FetchRange> prefetch;

    public FetchPlan(FetchRange demand, List<FetchRange> prefetch) {
        this.demand = demand;
        this.prefetch = prefetch == null ? Collections.emptyList() : prefetch;
    }

    public FetchPlan(FetchRange demand) {
        this(demand, Collections.emptyList());
    }

    public FetchRange demand() {
        return demand;
    }

    /**
     * Speculative look-ahead ranges (may be empty). The async reader schedules these; the sync (S2) path ignores them.
     */
    public List<FetchRange> prefetch() {
        return prefetch;
    }
}
