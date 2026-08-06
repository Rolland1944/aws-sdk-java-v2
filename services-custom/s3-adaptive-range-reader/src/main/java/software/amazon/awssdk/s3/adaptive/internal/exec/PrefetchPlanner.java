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

import java.util.ArrayList;
import java.util.List;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.PolicyName;

/**
 * {@code s3a_prefetch}: block-aligned demand read plus speculative prefetch of the next {@code prefetchDepth} blocks.
 * With {@code prefetchDepth == 0} it degrades to the synchronous S2 behaviour (block-aligned demand, no look-ahead).
 * A modest block (default 1 MiB, tunable 1&ndash;4 MiB) keeps read amplification bounded rather than copying the
 * simulator's aggressive 8 MiB &times; 8 lookahead.
 */
@SdkInternalApi
public final class PrefetchPlanner implements PolicyPlanner {

    private static final long DEFAULT_BLOCK_SIZE = 1024L * 1024;

    private final long blockSize;
    private final int prefetchDepth;

    public PrefetchPlanner() {
        this(DEFAULT_BLOCK_SIZE, 0);
    }

    public PrefetchPlanner(long blockSize, int prefetchDepth) {
        this.blockSize = blockSize > 0 ? blockSize : DEFAULT_BLOCK_SIZE;
        this.prefetchDepth = Math.max(0, prefetchDepth);
    }

    @Override
    public PolicyName policy() {
        return PolicyName.S3A_PREFETCH;
    }

    @Override
    public FetchPlan plan(long position, int length) {
        long blockStart = PolicyPlanner.alignDown(position, blockSize);
        FetchRange demand = new FetchRange(blockStart, blockStart + blockSize);
        if (prefetchDepth == 0) {
            return new FetchPlan(demand);
        }
        List<FetchRange> ahead = new ArrayList<>(prefetchDepth);
        for (int i = 1; i <= prefetchDepth; i++) {
            long start = blockStart + i * blockSize;
            ahead.add(new FetchRange(start, start + blockSize));
        }
        return new FetchPlan(demand, ahead);
    }
}
