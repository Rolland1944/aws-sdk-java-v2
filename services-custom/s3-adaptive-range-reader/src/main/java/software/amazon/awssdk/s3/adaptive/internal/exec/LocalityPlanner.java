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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.PolicyName;

/**
 * {@code template_locality}: on a revisited 256 KiB page widen the fetch to the whole page (so future reads hit
 * cache); otherwise a small 64 KiB readahead. Ports {@code TemplateLocalityCachePolicy.on_read}.
 *
 * <p>When run in the async reader ({@code prefetchDepth > 0}) a page-mode read also speculatively prefetches the next
 * {@code prefetchDepth} pages, exploiting the observed sequential-within-locality pattern. Page-visit counts advance
 * once per read (they are only meaningful on a miss, matching the simulator where a covering hit returns first).
 */
@SdkInternalApi
public final class LocalityPlanner implements PolicyPlanner {

    private static final long PAGE_SIZE = 256L * 1024;
    private static final long SMALL_READAHEAD = 64L * 1024;
    private static final int REVISIT_THRESHOLD = 2;

    private final Map<Long, Integer> seenPages = new HashMap<>();
    private final int prefetchDepth;

    public LocalityPlanner() {
        this(0);
    }

    public LocalityPlanner(int prefetchDepth) {
        this.prefetchDepth = Math.max(0, prefetchDepth);
    }

    @Override
    public PolicyName policy() {
        return PolicyName.TEMPLATE_LOCALITY;
    }

    @Override
    public FetchPlan plan(long position, int length) {
        long page = PolicyPlanner.alignDown(position, PAGE_SIZE);
        int visits = seenPages.merge(page, 1, Integer::sum);
        if (visits >= REVISIT_THRESHOLD) {
            FetchRange demand = new FetchRange(page, page + PAGE_SIZE);
            if (prefetchDepth == 0) {
                return new FetchPlan(demand);
            }
            List<FetchRange> ahead = new ArrayList<>(prefetchDepth);
            for (int i = 1; i <= prefetchDepth; i++) {
                long start = page + i * PAGE_SIZE;
                ahead.add(new FetchRange(start, start + PAGE_SIZE));
            }
            return new FetchPlan(demand, ahead);
        }
        return new FetchPlan(new FetchRange(position, position + Math.max(length, SMALL_READAHEAD)));
    }
}
