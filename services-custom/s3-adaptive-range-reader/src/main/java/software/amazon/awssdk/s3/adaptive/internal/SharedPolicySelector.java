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

/**
 * A per-app, cross-object policy selector shared by all of an app's readers. In S3 the feature window and hysteresis
 * are hoisted from per-reader to per-app so the 64-read window spans every object the app touches (restoring the
 * cross-object statistics the model was trained on, e.g. {@code distinct_obj_ratio}). Because an engine may read many
 * objects concurrently on different threads, implementations must serialize {@link #onRead(IoRequest)}.
 */
@SdkInternalApi
public interface SharedPolicySelector {

    PolicyName onRead(IoRequest req);

    PolicyName currentPolicy();
}
