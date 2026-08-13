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

import java.net.URI;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

/**
 * Environment-backed connection details for manually-run MinIO replay tools.
 *
 * <p>The values intentionally come only from the process environment so credentials cannot accidentally be persisted
 * in Maven arguments, benchmark reports, or source control.
 */
final class MinioEnvironment {

    static final String ENDPOINT = "S3ARR_ENDPOINT";
    static final String BUCKET = "S3ARR_BUCKET";
    static final String ACCESS_KEY = "S3ARR_ACCESS_KEY";
    static final String SECRET_KEY = "S3ARR_SECRET_KEY";
    static final String REGION = "S3ARR_REGION";

    private final URI endpoint;
    private final String bucket;
    private final Region region;
    private final StaticCredentialsProvider credentials;

    private MinioEnvironment(URI endpoint, String bucket, Region region, StaticCredentialsProvider credentials) {
        this.endpoint = endpoint;
        this.bucket = bucket;
        this.region = region;
        this.credentials = credentials;
    }

    static MinioEnvironment fromEnvironment() {
        String endpoint = required(ENDPOINT);
        String bucket = required(BUCKET);
        String accessKey = required(ACCESS_KEY);
        String secretKey = required(SECRET_KEY);
        String region = System.getenv(REGION);
        return new MinioEnvironment(URI.create(endpoint), bucket,
                                    Region.of(region == null || region.trim().isEmpty() ? "us-east-1" : region.trim()),
                                    StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
    }

    String bucket() {
        return bucket;
    }

    URI endpoint() {
        return endpoint;
    }

    Region region() {
        return region;
    }

    StaticCredentialsProvider credentials() {
        return credentials;
    }

    static S3Configuration s3ConfigurationPublic() {
        return s3Configuration();
    }

    S3Client syncClient() {
        return S3Client.builder()
                       .endpointOverride(endpoint)
                       .region(region)
                       .credentialsProvider(credentials)
                       .serviceConfiguration(s3Configuration())
                       .build();
    }

    S3AsyncClient asyncClient() {
        return S3AsyncClient.builder()
                            .endpointOverride(endpoint)
                            .region(region)
                            .credentialsProvider(credentials)
                            .serviceConfiguration(s3Configuration())
                            .build();
    }

    private static S3Configuration s3Configuration() {
        return S3Configuration.builder()
                              .pathStyleAccessEnabled(true)
                              .checksumValidationEnabled(false)
                              .build();
    }

    private static String required(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Set environment variable " + name + " before running the MinIO tool");
        }
        return value.trim();
    }
}
