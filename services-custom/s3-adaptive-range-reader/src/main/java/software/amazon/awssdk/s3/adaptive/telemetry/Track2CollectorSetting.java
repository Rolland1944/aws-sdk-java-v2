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

package software.amazon.awssdk.s3.adaptive.telemetry;

import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.utils.SystemSetting;

/**
 * Settings for {@link Track2IoCollectorInterceptor}.
 *
 * <p>Each setting is readable as either a system property or an environment variable, which matters because the
 * collector is configured on engine executors: {@code spark.executorEnv.TRACK2_COLLECTOR_DIR} is usually easier to set
 * than threading a {@code -D} through {@code spark.executor.extraJavaOptions}.
 */
@SdkInternalApi
public enum Track2CollectorSetting implements SystemSetting {

    /** Set to {@code false} to load the interceptor but record nothing. */
    ENABLED("track2.collector.enabled", "TRACK2_COLLECTOR_ENABLED", "true"),

    /** Directory for NDJSON output; one file per JVM. Defaults to a {@code track2-io} directory under the temp dir. */
    DIR("track2.collector.dir", "TRACK2_COLLECTOR_DIR", null),

    /**
     * Comma-separated audit-context keys to lift out of the {@code Referer} header into each record.
     *
     * <p>{@code ji} (job id) and {@code ta} (task attempt id) are only populated by the S3A committers, so on a pure
     * read workload they are usually absent. The primary correlation key is therefore one the query engine injects
     * itself into S3A's {@code CommonAuditContext}, which S3A then serialises into the referrer alongside its own
     * fields; {@code sqlid} is the default name for that injected key.
     */
    CORRELATION_KEYS("track2.collector.correlationKeys", "TRACK2_COLLECTOR_CORRELATION_KEYS", "sqlid,ji,ta"),

    /** Fallback location for {@link #DIR}. Declared here so the value is read through the sanctioned accessor. */
    TEMP_DIR("java.io.tmpdir", null, "/tmp");

    private final String property;
    private final String environmentVariable;
    private final String defaultValue;

    Track2CollectorSetting(String property, String environmentVariable, String defaultValue) {
        this.property = property;
        this.environmentVariable = environmentVariable;
        this.defaultValue = defaultValue;
    }

    @Override
    public String property() {
        return property;
    }

    @Override
    public String environmentVariable() {
        return environmentVariable;
    }

    @Override
    public String defaultValue() {
        return defaultValue;
    }
}
