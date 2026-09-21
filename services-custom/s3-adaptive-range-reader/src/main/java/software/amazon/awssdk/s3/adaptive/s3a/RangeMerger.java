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

package software.amazon.awssdk.s3.adaptive.s3a;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Greedy D4 grouping: sort by start, grow a run while the hole is ≤ {@code g*},
 * the merged GET fits {@code maxSingleFetchBytes}, and waste/merged ≤ the cap.
 */
@SdkInternalApi
public final class RangeMerger {

    private RangeMerger() {
    }

    public static final class Span {
        public final long start;
        public final long endInclusive;

        public Span(long start, long endInclusive) {
            this.start = start;
            this.endInclusive = endInclusive;
        }

        public long length() {
            return endInclusive - start + 1;
        }
    }

    public static final class Group {
        public final long start;
        public final long endInclusive;
        public final List<Integer> members;
        public final long usefulBytes;
        public final long wasteBytes;

        Group(long start, long endInclusive, List<Integer> members, long usefulBytes) {
            this.start = start;
            this.endInclusive = endInclusive;
            this.members = Collections.unmodifiableList(members);
            this.usefulBytes = usefulBytes;
            this.wasteBytes = (endInclusive - start + 1) - usefulBytes;
        }

        public long length() {
            return endInclusive - start + 1;
        }
    }

    public static List<Group> group(List<Span> ranges, long gStar, long maxBytes, double maxWaste) {
        List<Integer> order = new ArrayList<Integer>(ranges.size());
        for (int i = 0; i < ranges.size(); i++) {
            order.add(i);
        }
        Collections.sort(order, new Comparator<Integer>() {
            @Override
            public int compare(Integer a, Integer b) {
                int byStart = Long.compare(ranges.get(a).start, ranges.get(b).start);
                return byStart != 0 ? byStart : Long.compare(ranges.get(a).endInclusive,
                                                             ranges.get(b).endInclusive);
            }
        });
        List<Group> out = new ArrayList<Group>();
        int i = 0;
        while (i < order.size()) {
            List<Integer> members = new ArrayList<Integer>();
            members.add(order.get(i));
            i++;
            while (i < order.size() && canAdd(ranges, members, order.get(i), gStar, maxBytes, maxWaste)) {
                members.add(order.get(i));
                i++;
            }
            out.add(toGroup(ranges, members));
        }
        return out;
    }

    static long usefulBytes(List<Span> ranges, List<Integer> members) {
        List<Span> sorted = new ArrayList<Span>(members.size());
        for (int idx : members) {
            sorted.add(ranges.get(idx));
        }
        Collections.sort(sorted, new Comparator<Span>() {
            @Override
            public int compare(Span a, Span b) {
                int byStart = Long.compare(a.start, b.start);
                return byStart != 0 ? byStart : Long.compare(a.endInclusive, b.endInclusive);
            }
        });
        long total = 0;
        long curS = sorted.get(0).start;
        long curE = sorted.get(0).endInclusive;
        for (int i = 1; i < sorted.size(); i++) {
            Span r = sorted.get(i);
            if (r.start <= curE + 1) {
                curE = Math.max(curE, r.endInclusive);
            } else {
                total += curE - curS + 1;
                curS = r.start;
                curE = r.endInclusive;
            }
        }
        return total + (curE - curS + 1);
    }

    private static boolean canAdd(List<Span> ranges, List<Integer> members, int next,
                                  long gStar, long maxBytes, double maxWaste) {
        Group current = toGroup(ranges, members);
        Span add = ranges.get(next);
        long gap = add.start > current.endInclusive + 1 ? add.start - current.endInclusive - 1 : 0;
        if (gap > gStar) {
            return false;
        }
        List<Integer> trial = new ArrayList<Integer>(members);
        trial.add(next);
        Group nextGroup = toGroup(ranges, trial);
        if (nextGroup.length() > maxBytes) {
            return false;
        }
        if (nextGroup.length() > 0 && nextGroup.wasteBytes > 0) {
            double waste = nextGroup.wasteBytes / (double) nextGroup.length();
            if (waste > maxWaste) {
                return false;
            }
        }
        return true;
    }

    private static Group toGroup(List<Span> ranges, List<Integer> members) {
        long start = Long.MAX_VALUE;
        long end = Long.MIN_VALUE;
        for (int idx : members) {
            Span s = ranges.get(idx);
            start = Math.min(start, s.start);
            end = Math.max(end, s.endInclusive);
        }
        return new Group(start, end, new ArrayList<Integer>(members), usefulBytes(ranges, members));
    }
}
