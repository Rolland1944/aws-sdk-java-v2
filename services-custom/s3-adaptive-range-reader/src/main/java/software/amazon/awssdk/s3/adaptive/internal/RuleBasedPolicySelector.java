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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.PolicyName;

/**
 * A hand-written rule {@link SharedPolicySelector} that routes each read to one of the four policy executors using
 * fixed IO-shape thresholds. It is the {@code template_auto} baseline: the same job as the learned
 * {@link AdaptivePolicySelector} (route among the same executors) but with hand-tuned rules instead of a decision
 * tree, so a benchmark can show whether the learned selector actually beats a sensible hand rule (not merely the
 * no-cache lower bound).
 *
 * <p>Ported from {@code prefetch_simulator.TemplateAutoPolicy.on_read}, adapted in two deliberate ways: (1) the
 * {@code query_id.startswith("retrieval")} check is dropped so routing stays label-free (no workload-oracle leakage,
 * matching how the decision tree sees only IO features); (2) page-visit counts are advanced on <i>every</i> read so a
 * revisited page actually engages the locality branch (the simulator only incremented them inside the locality
 * sub-policy, which made that branch practically unreachable under {@code template_auto}).
 *
 * <p>State is per object ({@code recent} offsets + {@code seenPages}), matching the simulator's per-object
 * {@code ObjectState}. Shared by all readers of an app, so {@code onRead}/{@code currentPolicy} are synchronized.
 */
@SdkInternalApi
public final class RuleBasedPolicySelector implements SharedPolicySelector {

    private static final long LARGE_READ = 128L * 1024;
    private static final long SEQ_MIN_READ = 32L * 1024;
    private static final long SMALL_READ = 16L * 1024;
    private static final long PAGE_SIZE = 256L * 1024;
    private static final double FORWARD_THRESHOLD = 0.7;
    private static final int RECENT_MAX = 8;

    private final Map<String, ObjState> objects = new HashMap<>();
    private PolicyName current;

    @Override
    public synchronized PolicyName onRead(IoRequest req) {
        ObjState obj = objects.computeIfAbsent(req.objectKey(), k -> new ObjState());
        long offset = req.offset();
        long length = req.length();
        long page = (offset / PAGE_SIZE) * PAGE_SIZE;

        PolicyName policy = classify(obj, offset, length, page);

        obj.recent.addLast(offset);
        if (obj.recent.size() > RECENT_MAX) {
            obj.recent.removeFirst();
        }
        obj.seenPages.add(page);
        current = policy;
        return policy;
    }

    private PolicyName classify(ObjState obj, long offset, long length, long page) {
        if (length >= LARGE_READ) {
            return PolicyName.TEMPLATE_MULTIMODAL;
        }
        if (obj.recent.size() >= 2 && forwardRatio(obj) >= FORWARD_THRESHOLD && length >= SEQ_MIN_READ) {
            return PolicyName.S3A_PREFETCH;
        }
        if (obj.seenPages.contains(page)) {
            return PolicyName.TEMPLATE_LOCALITY;
        }
        if (length <= SMALL_READ) {
            return PolicyName.S3A_RANDOM;
        }
        return PolicyName.S3A_PREFETCH;
    }

    private static double forwardRatio(ObjState obj) {
        long prev = Long.MIN_VALUE;
        int forward = 0;
        int pairs = 0;
        for (long off : obj.recent) {
            if (prev != Long.MIN_VALUE) {
                pairs++;
                if (off >= prev) {
                    forward++;
                }
            }
            prev = off;
        }
        return pairs == 0 ? 0.0 : (double) forward / pairs;
    }

    @Override
    public synchronized PolicyName currentPolicy() {
        return current;
    }

    private static final class ObjState {
        private final Deque<Long> recent = new ArrayDeque<>(RECENT_MAX + 1);
        private final Set<Long> seenPages = new HashSet<>();
    }
}
