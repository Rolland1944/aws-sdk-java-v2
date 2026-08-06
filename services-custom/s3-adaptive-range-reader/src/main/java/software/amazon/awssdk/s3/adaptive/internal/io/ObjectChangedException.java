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

package software.amazon.awssdk.s3.adaptive.internal.io;

import software.amazon.awssdk.annotations.SdkInternalApi;

/**
 * Thrown when an object mutates (version token changes) between the reader pinning a version and a subsequent ranged
 * GET, so that already-cached bytes can no longer be trusted. The reader surfaces this rather than silently returning
 * a mix of old and new bytes (PROJECT2 §7.2 version isolation).
 */
@SdkInternalApi
public class ObjectChangedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ObjectChangedException(String message) {
        super(message);
    }
}
