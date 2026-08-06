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
 * A single app's view onto the shared {@link GlobalBudget}: acquire/release cache bytes and inspect this app's usage
 * and reserved floor, without exposing other apps. Handed to the per-app cache so it can charge and refund the shared
 * budget under the app's identity.
 */
@SdkInternalApi
public final class AppBudgetLease {

    private final GlobalBudget budget;
    private final String appId;

    public AppBudgetLease(GlobalBudget budget, String appId) {
        this.budget = budget;
        this.appId = appId;
    }

    public String appId() {
        return appId;
    }

    public GlobalBudget globalBudget() {
        return budget;
    }

    public boolean acquire(long bytes) {
        return budget.acquire(appId, bytes);
    }

    public void release(long bytes) {
        budget.release(appId, bytes);
    }

    public long usage() {
        return budget.usage(appId);
    }

    public long reserved() {
        return budget.reserved(appId);
    }
}
