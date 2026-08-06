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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * The feature schema shared by {@link FeatureWindow} and the exported model. These values are the single source of
 * truth on the Java side; the loaded model JSON is validated against them so training and inference cannot silently
 * drift. Constants mirror {@code prefetch_simulator.py} (L700-716).
 */
@SdkInternalApi
public final class FeatureSchema {

    /** Feature vector length / order. MUST match the exported {@code feature_names}. */
    public static final List<String> FEATURE_NAMES = Collections.unmodifiableList(Arrays.asList(
        "log2_cur_size",
        "log2_med_size",
        "frac_small",
        "frac_large",
        "sequentiality",
        "forward_ratio",
        "log2_med_gap",
        "page_revisit_ratio",
        "distinct_obj_ratio",
        "cur_size_over_filesize"));

    public static final int FEATURE_COUNT = FEATURE_NAMES.size();

    /** Rolling history window size (reads observed before the current one). */
    public static final int WINDOW = 64;

    public static final long SMALL_READ_BYTES = 16L * 1024;
    public static final long LARGE_READ_BYTES = 128L * 1024;
    public static final long SEQ_GAP_BYTES = 64L * 1024;
    public static final long FEATURE_PAGE_SIZE = 256L * 1024;

    private FeatureSchema() {
    }
}
