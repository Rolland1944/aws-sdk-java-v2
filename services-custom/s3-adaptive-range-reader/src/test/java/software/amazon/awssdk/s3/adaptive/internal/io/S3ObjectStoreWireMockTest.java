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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import java.net.URI;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;

@WireMockTest
class S3ObjectStoreWireMockTest {

    private S3Client syncClient;
    private S3AsyncClient asyncClient;

    @BeforeEach
    void setUp(WireMockRuntimeInfo wireMock) {
        URI endpoint = URI.create("http://localhost:" + wireMock.getHttpPort());
        StaticCredentialsProvider credentials =
            StaticCredentialsProvider.create(AwsBasicCredentials.create("akid", "skid"));
        S3Configuration config = S3Configuration.builder()
                                                  .pathStyleAccessEnabled(true)
                                                  .checksumValidationEnabled(false)
                                                  .build();
        syncClient = S3Client.builder()
                             .httpClient(UrlConnectionHttpClient.builder().build())
                             .endpointOverride(endpoint)
                             .region(Region.US_EAST_1)
                             .credentialsProvider(credentials)
                             .serviceConfiguration(config)
                             .build();
        asyncClient = S3AsyncClient.builder()
                                   .httpClient(NettyNioAsyncHttpClient.builder().build())
                                   .endpointOverride(endpoint)
                                   .region(Region.US_EAST_1)
                                   .credentialsProvider(credentials)
                                   .serviceConfiguration(config)
                                   .build();
    }

    @AfterEach
    void closeClients() {
        if (syncClient != null) {
            syncClient.close();
        }
        if (asyncClient != null) {
            asyncClient.close();
        }
    }

    @Test
    void syncStoreSendsHeadRangeAndVersionRequests() {
        stubHead();
        stubRange("bytes=2-6", 206, "hello");
        S3ClientObjectStore store = new S3ClientObjectStore(syncClient, "bucket", "v1");

        ObjectMeta meta = store.head("key");
        assertThat(meta.contentLength()).isEqualTo(10);
        assertThat(meta.versionToken()).isEqualTo("\"etag-1\"");
        assertThat(store.getRange("key", 2, 7, meta.versionToken())).isEqualTo("hello".getBytes());
        assertThat(store.getRange("key", 3, 3, meta.versionToken())).isEmpty();

        verify(headRequested());
        verify(getRequested());
    }

    @Test
    void asyncStoreSendsHeadRangeAndMapsPreconditionFailure() {
        stubHead();
        stubRange("bytes=2-6", 206, "hello");
        S3AsyncClientObjectStore store = new S3AsyncClientObjectStore(asyncClient, "bucket", "v1");

        ObjectMeta meta = store.head("key");
        assertThat(store.getRange("key", 2, 7, meta.versionToken()).join()).isEqualTo("hello".getBytes());
        assertThat(store.getRange("key", 3, 3, meta.versionToken()).join()).isEmpty();
        verify(headRequested());
        verify(getRequested());

        stubRange("bytes=0-0", 412, "<Error><Code>PreconditionFailed</Code><Message>changed</Message></Error>");
        assertThatThrownBy(() -> store.getRange("key", 0, 1, meta.versionToken()).join())
            .isInstanceOf(CompletionException.class)
            .hasCauseInstanceOf(ObjectChangedException.class);
    }

    private static void stubHead() {
        stubFor(head(urlEqualTo("/bucket/key?versionId=v1"))
                    .willReturn(aResponse().withStatus(200)
                                           .withHeader("Content-Length", "10")
                                           .withHeader("ETag", "\"etag-1\"")));
    }

    private static void stubRange(String range, int status, String body) {
        stubFor(get(urlEqualTo("/bucket/key?versionId=v1"))
                    .withHeader("Range", equalTo(range))
                    .withHeader("If-Match", equalTo("\"etag-1\""))
                    .willReturn(aResponse().withStatus(status).withBody(body)));
    }

    private static RequestPatternBuilder headRequested() {
        return headRequested("/bucket/key?versionId=v1");
    }

    private static RequestPatternBuilder headRequested(String url) {
        return com.github.tomakehurst.wiremock.client.WireMock.headRequestedFor(urlEqualTo(url));
    }

    private static RequestPatternBuilder getRequested() {
        return com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor(urlEqualTo("/bucket/key?versionId=v1"))
                                                              .withHeader("Range", equalTo("bytes=2-6"))
                                                              .withHeader("If-Match", equalTo("\"etag-1\""));
    }
}
