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

package software.amazon.awssdk.s3.adaptive;

import software.amazon.awssdk.annotations.SdkPublicApi;

/**
 * Which "policy brain" an {@link AdaptiveReaderRuntime} uses to route each read to a policy executor. Both modes pick
 * among the same four executors and run over the same per-app cache / work-conserving budget, so they are directly
 * comparable; only the routing logic differs.
 */
@SdkPublicApi
public enum SelectorMode {

    /** The offline-trained decision tree over the 10-dim IO feature window (the adaptive selector). */
    DECISION_TREE,

    /** A hand-written {@code template_auto}-style rule over fixed IO-shape thresholds (comparison baseline). */
    TEMPLATE_AUTO
}
