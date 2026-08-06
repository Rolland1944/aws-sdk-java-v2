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
import java.util.Arrays;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Bounded (64-read) rolling window of recent {@link IoRequest}s that derives the 10 purely IO-based features used by
 * the policy model. This is a 1:1 port of {@code prefetch_simulator.extract_features} (PROJECT2 §7.2): the feature
 * vector for a read is computed over the history <i>before</i> that read plus the read itself, then the read is
 * appended to the window. No query id / workload label is ever used, so there is no oracle leakage.
 *
 * <p>Not thread-safe: one instance tracks a single logical read stream.
 */
@SdkInternalApi
public final class FeatureWindow {

    private final int capacity;
    private final Deque<IoRequest> history;

    // Reusable scratch buffers so featuresFor() allocates only its result array on the hot path. Sized for the full
    // window plus the current read. This makes FeatureWindow single-threaded by contract (one stream per instance).
    private final long[] sizeScratch;
    private final long[] gapScratch;
    private final Set<String> pageScratch;
    private final Set<String> objectScratch;

    public FeatureWindow() {
        this(FeatureSchema.WINDOW);
    }

    public FeatureWindow(int capacity) {
        this.capacity = capacity;
        this.history = new ArrayDeque<>(capacity);
        this.sizeScratch = new long[capacity + 1];
        this.gapScratch = new long[capacity + 1];
        this.pageScratch = new HashSet<>(2 * (capacity + 1));
        this.objectScratch = new HashSet<>(2 * (capacity + 1));
    }

    /**
     * Append a read to the window, evicting the oldest if the capacity is exceeded. Call this <i>after</i>
     * {@link #featuresFor(IoRequest)} for the same read, matching the Python order (extract, then append).
     */
    public void add(IoRequest req) {
        if (history.size() >= capacity) {
            history.removeFirst();
        }
        history.addLast(req);
    }

    /**
     * Clear the window (e.g. when starting a new independent read stream / segment).
     */
    public void reset() {
        history.clear();
    }

    public int size() {
        return history.size();
    }

    /**
     * Compute the 10-dimensional feature vector for {@code req} over the current window + {@code req}. Does not mutate
     * the window.
     */
    public double[] featuresFor(IoRequest req) {
        // window = history (oldest -> newest) followed by the current request. Iterated once, with reusable scratch
        // buffers, so the only hot-path allocation is the returned feature array.
        int n = history.size() + 1;
        String objectKey = req.objectKey();

        long curSize = Math.max(1L, req.length());

        pageScratch.clear();
        objectScratch.clear();
        int smallCount = 0;
        int largeCount = 0;
        int revisits = 0;
        // Same-object consecutive pairs for sequentiality / forward-ratio / gap features.
        IoRequest prevSameObject = null;
        int pairCount = 0;
        int seqCount = 0;
        int fwdCount = 0;

        Iterator<IoRequest> it = history.iterator();
        for (int i = 0; i < n; i++) {
            IoRequest r = i < n - 1 ? it.next() : req;
            long len = r.length();
            sizeScratch[i] = len;
            if (len <= FeatureSchema.SMALL_READ_BYTES) {
                smallCount++;
            }
            if (len >= FeatureSchema.LARGE_READ_BYTES) {
                largeCount++;
            }

            if (r.objectKey().equals(objectKey)) {
                if (prevSameObject != null) {
                    long gap = r.offset() - prevSameObject.end();
                    if (gap >= 0 && gap <= FeatureSchema.SEQ_GAP_BYTES) {
                        seqCount++;
                    }
                    if (r.offset() >= prevSameObject.offset()) {
                        fwdCount++;
                    }
                    gapScratch[pairCount++] = Math.abs(gap);
                }
                prevSameObject = r;
            }

            String pageKey = r.objectKey() + '\u0000' + (r.offset() / FeatureSchema.FEATURE_PAGE_SIZE);
            if (!pageScratch.add(pageKey)) {
                revisits++;
            }
            objectScratch.add(r.objectKey());
        }

        double medSize = median(sizeScratch, n);
        double fracSmall = (double) smallCount / n;
        double fracLarge = (double) largeCount / n;

        double seq;
        double fwd;
        double medGap;
        if (pairCount >= 1) {
            seq = (double) seqCount / pairCount;
            fwd = (double) fwdCount / pairCount;
            medGap = median(gapScratch, pairCount);
        } else {
            seq = 0.0;
            fwd = 1.0;
            medGap = 0.0;
        }

        double pageRevisit = (double) revisits / n;
        double distinctObj = (double) objectScratch.size() / n;

        double sizeRatio = 0.0;
        Double fsize = req.fileSize();
        if (fsize != null && !fsize.isNaN() && fsize > 0) {
            sizeRatio = Math.min(1.0, (double) req.length() / fsize);
        }

        double[] features = new double[FeatureSchema.FEATURE_COUNT];
        features[0] = log2(curSize);
        features[1] = log2(Math.max(1.0, medSize));
        features[2] = fracSmall;
        features[3] = fracLarge;
        features[4] = seq;
        features[5] = fwd;
        features[6] = log2(1.0 + medGap);
        features[7] = pageRevisit;
        features[8] = distinctObj;
        features[9] = sizeRatio;
        return features;
    }

    private static double log2(double value) {
        return Math.log(value) / Math.log(2.0);
    }

    /**
     * numpy.median semantics over the first {@code len} elements of {@code values}: the middle element for odd length,
     * or the average of the two middle elements for even length. Sorts that prefix in place (the scratch buffer is
     * rebuilt on every call, so in-place sorting is safe).
     */
    private static double median(long[] values, int len) {
        if (len == 0) {
            return 0.0;
        }
        Arrays.sort(values, 0, len);
        int mid = len / 2;
        if ((len & 1) == 1) {
            return (double) values[mid];
        }
        return (values[mid - 1] + values[mid]) / 2.0;
    }
}
