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

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.PolicyName;
import software.amazon.awssdk.s3.adaptive.ReaderMetrics;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectMeta;
import software.amazon.awssdk.s3.adaptive.internal.io.ObjectStore;
import software.amazon.awssdk.s3.adaptive.internal.metrics.MetricsRecorder;

/**
 * The feature-flag-off reader: every read is an exact demand ranged GET, with no policy selection and no cache. The
 * bytes returned are identical to a plain {@code getObject} range request, so enabling the flag cannot change
 * correctness. Metrics are still tracked (policy is always {@code null}).
 */
@SdkInternalApi
public final class PassthroughRangeReader extends AbstractRangeReader {

    private final ObjectStore store;
    private final String key;
    private final String versionToken;
    private final MetricsRecorder metrics = new MetricsRecorder();

    public PassthroughRangeReader(ObjectStore store, String key) {
        this(store, key, store.head(key));
    }

    private PassthroughRangeReader(ObjectStore store, String key, ObjectMeta meta) {
        super(meta.contentLength());
        this.store = store;
        this.key = key;
        this.versionToken = meta.versionToken();
    }

    @Override
    protected int readAt(long position, byte[] dst, int offset, int length) {
        long start = System.nanoTime();
        byte[] data = store.getRange(key, position, position + length, versionToken);
        metrics.recordRemoteFetch(data.length);
        System.arraycopy(data, 0, dst, offset, length);
        metrics.recordRead(null, length, System.nanoTime() - start, false);
        return length;
    }

    @Override
    public ReaderMetrics metrics() {
        return metrics.snapshot();
    }

    @Override
    public PolicyName currentPolicy() {
        return null;
    }
}
