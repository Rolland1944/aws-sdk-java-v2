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

package software.amazon.awssdk.s3.adaptive.internal.exec;

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.s3.adaptive.PolicyName;

/**
 * Turns a logical read into physical ranged fetches for one policy: consult the page cache, otherwise fetch a
 * policy-shaped range, cache it, and copy the requested bytes into the caller's buffer. Implementations are
 * synchronous and hold no async/prefetch state (deferred to S3).
 */
@SdkInternalApi
public interface PolicyExecutor {

    /**
     * The policy this executor implements.
     */
    PolicyName policy();

    /**
     * Serve {@code [position, position + length)} into {@code dst} at {@code dstOffset}. The caller guarantees
     * {@code length > 0} and {@code position + length <= objectSize}.
     *
     * @return the number of bytes written (always {@code length}).
     */
    int serve(long position, byte[] dst, int dstOffset, int length);
}
