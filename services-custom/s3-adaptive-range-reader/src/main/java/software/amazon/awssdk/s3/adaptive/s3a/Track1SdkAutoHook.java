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

import java.util.List;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.core.client.builder.SdkClientBuilder;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.s3.adaptive.telemetry.Track2IoCollectorInterceptor;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Engine-agnostic hook invoked on {@code DefaultS3ClientBuilder} construction when
 * {@code -Dtrack1.sdk.auto=true} is enabled (e.g. Trino native S3 filesystem).
 *
 * <p>Attaches {@link Track2IoCollectorInterceptor} (if not already registered) and wraps
 * the constructed {@link S3Client} with {@link PassthroughS3Clients#wrapSync} so both
 * Track 1 caching and Track 2 NDJSON telemetry work without modifying the query engine.
 */
@SdkPublicApi
public final class Track1SdkAutoHook {

    private Track1SdkAutoHook() {
    }

    public static void beforeBuild(Object builderObj) {
        if (!Boolean.getBoolean("track1.sdk.auto") || !(builderObj instanceof SdkClientBuilder)) {
            return;
        }
        try {
            SdkClientBuilder<?, ?> builder = (SdkClientBuilder<?, ?>) builderObj;
            ClientOverrideConfiguration existing = builder.overrideConfiguration();
            List<ExecutionInterceptor> interceptors =
                existing == null ? null : existing.executionInterceptors();
            if (interceptors != null) {
                for (ExecutionInterceptor interceptor : interceptors) {
                    if (interceptor instanceof Track2IoCollectorInterceptor) {
                        return;
                    }
                }
            }
            ClientOverrideConfiguration.Builder next =
                existing == null ? ClientOverrideConfiguration.builder() : existing.toBuilder();
            next.addExecutionInterceptor(new Track2IoCollectorInterceptor());
            builder.overrideConfiguration(next.build());
        } catch (Throwable ignored) {
            // Never break S3Client construction if telemetry attachment fails.
        }
    }

    public static Object wrapClient(Object client) {
        if (!Boolean.getBoolean("track1.sdk.auto") || client == null) {
            return client;
        }
        try {
            Track1S3aRuntime runtime = Track1S3aRuntime.shared();
            if (client instanceof S3Client) {
                S3Client raw = (S3Client) client;
                runtime.registerSync(raw);
                return PassthroughS3Clients.wrapSync(raw, Track1S3aProbe.shared(), runtime.pipeline());
            }
            if (client instanceof S3AsyncClient) {
                S3AsyncClient raw = (S3AsyncClient) client;
                runtime.registerAsync(raw);
                return PassthroughS3Clients.wrapAsync(raw, Track1S3aProbe.shared());
            }
        } catch (Throwable ignored) {
            // Fall back to unwrapped raw client on any unexpected linkage error.
        }
        return client;
    }
}
