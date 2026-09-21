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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.s3.adaptive.internal.RuntimeConfig;

class Track1S3aRuntimeTest {

    @Test
    void snapshotJsonExportsCountersAndHeap() {
        Track1S3aRuntime runtime = new Track1S3aRuntime(
            RuntimeConfig.builder().d1Enabled(true).d2Enabled(true).d4Enabled(true)
                         .d2WaitWindowMicros(50).build());
        try {
            runtime.stats().cacheHit(128);
            runtime.stats().cacheMiss();
            runtime.stats().remoteGet();
            runtime.stats().merged(2, 16);
            runtime.stats().queueSubmitted();
            runtime.stats().queueBatch(2);
            runtime.stats().sameObjectGroup(2);
            runtime.stats().mergeableGroup();
            runtime.stats().rejectAdmit();
            runtime.stats().teedGet();
            String json = runtime.snapshotJson();
            assertThat(json).contains("\"d1\":true");
            assertThat(json).contains("\"d2\":true");
            assertThat(json).contains("\"d4\":true");
            assertThat(json).contains("\"async_client_available\":false");
            assertThat(json).contains("\"d2_wait_us\":50");
            assertThat(json).contains("\"cache_hits\":1");
            assertThat(json).contains("\"cache_useful_bytes\":128");
            assertThat(json).contains("\"merged_gets\":1");
            assertThat(json).contains("\"wasted_bytes\":16");
            assertThat(json).contains("\"queue_submissions\":1");
            assertThat(json).contains("\"queue_batches\":1");
            assertThat(json).contains("\"queue_singleton_batches\":0");
            assertThat(json).contains("\"same_object_multi_ticket_groups\":1");
            assertThat(json).contains("\"mergeable_groups\":1");
            assertThat(json).contains("\"max_batch_size\":2");
            assertThat(json).contains("\"max_same_object_group_size\":2");
            assertThat(json).contains("\"d1_admit_max_bytes\":");
            assertThat(json).contains("\"d1_adaptive\":false");
            assertThat(json).contains("\"d1_mode\":\"observe\"");
            assertThat(json).contains("\"d1_u_h\":0");
            assertThat(json).contains("\"d1_r_h\":0");
            assertThat(json).contains("\"d1_r_admit\":0");
            assertThat(json).contains("\"d1_horizon_bytes\":");
            assertThat(json).contains("\"admit_rejected\":1");
            assertThat(json).contains("\"teed_gets\":1");
            assertThat(json).contains("\"teed_bytes\":");
            assertThat(json).contains("\"gc_ms\":");
            assertThat(json).contains("\"evicted_bytes\":");
            assertThat(json).contains("\"peak_heap_bytes\":");
        } finally {
            runtime.close();
        }
    }
}
