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

package software.amazon.awssdk.s3.adaptive.internal.budget;

import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * A per-app cache that the {@link GlobalBudget} can call back into to reclaim space. Implementations evict their own
 * least-recently-used blocks. The budget already adjusts the app's accounted usage by the returned amount, so the
 * evictor must NOT touch the budget itself (only its own backing store).
 */
@SdkInternalApi
public interface CacheEvictor {

    /**
     * Evict least-recently-used blocks totalling at least {@code bytes} (or all blocks if fewer bytes are cached).
     *
     * @return the number of bytes actually freed.
     */
    long evictLru(long bytes);
}
