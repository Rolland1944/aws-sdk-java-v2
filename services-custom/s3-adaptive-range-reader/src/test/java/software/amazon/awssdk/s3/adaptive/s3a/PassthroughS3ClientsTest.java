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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.head;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.s3.adaptive.internal.RuntimeConfig;
import software.amazon.awssdk.services.s3.model.S3Exception;

@WireMockTest
class PassthroughS3ClientsTest {

    private S3Client rawSync;
    private S3AsyncClient rawAsync;

    @BeforeEach
    void setUp(WireMockRuntimeInfo wireMock) {
        Track1S3aProbe.shared().reset();
        Track1S3aRuntime.resetShared(RuntimeConfig.builder().build());
        URI endpoint = URI.create("http://localhost:" + wireMock.getHttpPort());
        StaticCredentialsProvider credentials =
            StaticCredentialsProvider.create(AwsBasicCredentials.create("akid", "skid"));
        S3Configuration config = S3Configuration.builder()
                                                .pathStyleAccessEnabled(true)
                                                .checksumValidationEnabled(false)
                                                .build();
        rawSync = S3Client.builder()
                          .httpClient(UrlConnectionHttpClient.builder().build())
                          .endpointOverride(endpoint)
                          .region(Region.US_EAST_1)
                          .credentialsProvider(credentials)
                          .serviceConfiguration(config)
                          .build();
        rawAsync = S3AsyncClient.builder()
                                .httpClient(NettyNioAsyncHttpClient.builder().build())
                                .endpointOverride(endpoint)
                                .region(Region.US_EAST_1)
                                .credentialsProvider(credentials)
                                .serviceConfiguration(config)
                                .build();
    }

    @AfterEach
    void tearDown() {
        if (rawSync != null) {
            rawSync.close();
        }
        if (rawAsync != null) {
            rawAsync.close();
        }
        Track1S3aProbe.shared().reset();
    }

    @Test
    void parseRange_readsInclusiveBounds() {
        assertThat(Track1S3aProbe.parseRange("bytes=0-99")).containsExactly(0L, 99L);
        assertThat(Track1S3aProbe.parseRange("bytes=4096-")).containsExactly(4096L, -1L);
        assertThat(Track1S3aProbe.parseRange(null)).containsExactly(-1L, -1L);
        assertThat(Track1S3aProbe.parseRange("bytes=99-0")).containsExactly(-1L, -1L);
    }

    @Test
    void syncGetObject_returnsDelegateBytesAndRecordsRange() throws Exception {
        stubRange("bytes=2-6", 206, "hello");
        S3Client wrapped = PassthroughS3Clients.wrapSync(rawSync, Track1S3aProbe.shared());
        GetObjectRequest req = GetObjectRequest.builder()
                                               .bucket("bucket")
                                               .key("key")
                                               .range("bytes=2-6")
                                               .versionId("v1")
                                               .build();
        try (ResponseInputStream<GetObjectResponse> in = wrapped.getObject(req)) {
            assertThat(readAll(in)).isEqualTo("hello".getBytes());
        }
        assertThat(Track1S3aProbe.shared().syncGets()).isEqualTo(1);
        assertThat(Track1S3aProbe.shared().asyncGets()).isZero();
        Track1S3aProbe.Record rec = Track1S3aProbe.shared().snapshot().get(0);
        assertThat(rec.client).isEqualTo("sync");
        assertThat(rec.bucket).isEqualTo("bucket");
        assertThat(rec.key).isEqualTo("key");
        assertThat(rec.rangeStart).isEqualTo(2);
        assertThat(rec.rangeEndInclusive).isEqualTo(6);
        assertThat(rec.versionId).isEqualTo("v1");
        assertThat(rec.status).isEqualTo("ok");
        assertThat(PassthroughS3Clients.unwrapSync(wrapped)).isSameAs(rawSync);
    }

    @Test
    void syncHead_isNotRecordedAsGet() {
        stubFor(head(urlEqualTo("/bucket/key"))
                    .willReturn(aResponse().withStatus(200)
                                           .withHeader("Content-Length", "10")
                                           .withHeader("ETag", "\"etag-1\"")));
        S3Client wrapped = PassthroughS3Clients.wrapSync(rawSync, Track1S3aProbe.shared());
        assertThat(wrapped.headObject(HeadObjectRequest.builder()
                                                       .bucket("bucket")
                                                       .key("key")
                                                       .build())
                          .contentLength()).isEqualTo(10L);
        assertThat(Track1S3aProbe.shared().syncGets()).isZero();
    }

    @Test
    void syncGetObject_recordsExceptionWithoutSwallowingIt() {
        stubFor(get(urlEqualTo("/bucket/missing"))
                    .willReturn(aResponse().withStatus(404)
                                           .withBody("<Error><Code>NoSuchKey</Code></Error>")));
        S3Client wrapped = PassthroughS3Clients.wrapSync(rawSync, Track1S3aProbe.shared());
        GetObjectRequest req = GetObjectRequest.builder().bucket("bucket").key("missing").build();
        assertThatThrownBy(() -> wrapped.getObject(req)).isInstanceOf(S3Exception.class);
        assertThat(Track1S3aProbe.shared().exceptions()).isEqualTo(1);
        assertThat(Track1S3aProbe.shared().snapshot().get(0).status).isEqualTo("exception");
    }

    @Test
    void asyncGetObject_returnsDelegateBytesAndRecordsRange() throws Exception {
        stubRange("bytes=0-4", 206, "abcde");
        S3AsyncClient wrapped = PassthroughS3Clients.wrapAsync(rawAsync, Track1S3aProbe.shared());
        GetObjectRequest req = GetObjectRequest.builder()
                                               .bucket("bucket")
                                               .key("key")
                                               .range("bytes=0-4")
                                               .build();
        byte[] body = wrapped.getObject(req, AsyncResponseTransformer.toBytes()).join()
                             .asByteArray();
        assertThat(body).isEqualTo("abcde".getBytes());
        awaitProbe(() -> Track1S3aProbe.shared().asyncGets() >= 1);
        assertThat(Track1S3aProbe.shared().asyncGets()).isEqualTo(1);
        assertThat(Track1S3aProbe.shared().syncGets()).isZero();
        Track1S3aProbe.Record rec = Track1S3aProbe.shared().snapshot().get(0);
        assertThat(rec.client).isEqualTo("async");
        assertThat(rec.rangeStart).isEqualTo(0);
        assertThat(rec.rangeEndInclusive).isEqualTo(4);
        assertThat(rec.status).isEqualTo("ok");
        assertThat(PassthroughS3Clients.unwrapAsync(wrapped)).isSameAs(rawAsync);
    }

    @Test
    void asyncGetObject_recordsFailedFuture() throws Exception {
        stubFor(get(urlEqualTo("/bucket/missing"))
                    .willReturn(aResponse().withStatus(404)
                                           .withBody("<Error><Code>NoSuchKey</Code></Error>")));
        S3AsyncClient wrapped = PassthroughS3Clients.wrapAsync(rawAsync, Track1S3aProbe.shared());
        GetObjectRequest req = GetObjectRequest.builder().bucket("bucket").key("missing").build();
        assertThatThrownBy(() -> wrapped.getObject(req, AsyncResponseTransformer.toBytes()).join())
            .isInstanceOf(CompletionException.class);
        awaitProbe(() -> Track1S3aProbe.shared().exceptions() >= 1);
        assertThat(Track1S3aProbe.shared().exceptions()).isEqualTo(1);
        assertThat(Track1S3aProbe.shared().snapshot().get(0).client).isEqualTo("async");
    }

    @Test
    void wrapIsIdempotent() {
        S3Client once = PassthroughS3Clients.wrapSync(rawSync, Track1S3aProbe.shared());
        S3Client twice = PassthroughS3Clients.wrapSync(once, Track1S3aProbe.shared());
        assertThat(twice).isSameAs(once);
        S3AsyncClient asyncOnce = PassthroughS3Clients.wrapAsync(rawAsync, Track1S3aProbe.shared());
        S3AsyncClient asyncTwice = PassthroughS3Clients.wrapAsync(asyncOnce, Track1S3aProbe.shared());
        assertThat(asyncTwice).isSameAs(asyncOnce);
    }

    @Test
    void getObjectAsBytesAndTransformer_hitD1CacheOnRepeat() {
        stubFor(get(urlPathEqualTo("/bucket/key"))
                    .withHeader("Range", equalTo("bytes=0-4"))
                    .willReturn(aResponse().withStatus(206)
                                           .withHeader("ETag", "\"etag-1\"")
                                           .withBody("hello")));
        RuntimeConfig d1Cfg = RuntimeConfig.builder()
                                           .d1Enabled(true)
                                           .d1AdmitMaxBytes(256 * 1024)
                                           .globalCacheBytes(16L * 1024 * 1024)
                                           .perAppReservedBytes(16L * 1024 * 1024)
                                           .build();
        Track1S3aRuntime runtime = Track1S3aRuntime.resetShared(d1Cfg);
        S3Client wrapped = PassthroughS3Clients.wrapSync(rawSync, Track1S3aProbe.shared(), runtime.pipeline());
        GetObjectRequest req = GetObjectRequest.builder()
                                               .bucket("bucket")
                                               .key("key")
                                               .range("bytes=0-4")
                                               .build();
        ResponseBytes<GetObjectResponse> miss = wrapped.getObjectAsBytes(req);
        assertThat(miss.asByteArray()).isEqualTo("hello".getBytes());
        assertThat(runtime.stats().cacheMisses()).isEqualTo(1);
        assertThat(runtime.stats().cacheHits()).isZero();

        ResponseBytes<GetObjectResponse> hit = wrapped.getObject(req, ResponseTransformer.toBytes());
        assertThat(hit.asByteArray()).isEqualTo("hello".getBytes());
        assertThat(hit.response().eTag()).isEqualTo("\"etag-1\"");
        assertThat(runtime.stats().cacheHits()).isEqualTo(1);
        assertThat(runtime.stats().remoteGets()).isEqualTo(1);
    }

    @Test
    void suffixRangeReadTail_cachesAndHitsOnRepeat() throws Exception {
        stubFor(get(urlPathEqualTo("/bucket/parquet"))
                    .withHeader("Range", equalTo("bytes=-5"))
                    .willReturn(aResponse().withStatus(206)
                                           .withHeader("ETag", "\"etag-tail\"")
                                           .withHeader("Content-Range", "bytes 95-99/100")
                                           .withBody("PAR1!")));
        RuntimeConfig d1Cfg = RuntimeConfig.builder()
                                           .d1Enabled(true)
                                           .d1AdmitMaxBytes(256 * 1024)
                                           .globalCacheBytes(16L * 1024 * 1024)
                                           .perAppReservedBytes(16L * 1024 * 1024)
                                           .build();
        Track1S3aRuntime runtime = Track1S3aRuntime.resetShared(d1Cfg);
        S3Client wrapped = PassthroughS3Clients.wrapSync(rawSync, Track1S3aProbe.shared(), runtime.pipeline());
        GetObjectRequest tailReq = GetObjectRequest.builder()
                                                   .bucket("bucket")
                                                   .key("parquet")
                                                   .range("bytes=-5")
                                                   .build();
        try (ResponseInputStream<GetObjectResponse> first = wrapped.getObject(tailReq)) {
            assertThat(readAll(first)).isEqualTo("PAR1!".getBytes());
        }
        assertThat(runtime.stats().cacheMisses()).isEqualTo(1);
        assertThat(runtime.stats().cacheHits()).isZero();

        try (ResponseInputStream<GetObjectResponse> second = wrapped.getObject(tailReq)) {
            assertThat(readAll(second)).isEqualTo("PAR1!".getBytes());
        }
        assertThat(runtime.stats().cacheHits()).isEqualTo(1);
        assertThat(runtime.stats().remoteGets()).isEqualTo(1);
    }

    @Test
    void sdkAutoHook_attachesInterceptorOnceAndWrapsClient(WireMockRuntimeInfo wireMock) {
        String prev = System.getProperty("track1.sdk.auto");
        System.setProperty("track1.sdk.auto", "true");
        try {
            software.amazon.awssdk.services.s3.S3ClientBuilder builder =
                S3Client.builder()
                        .httpClient(UrlConnectionHttpClient.builder().build())
                        .endpointOverride(URI.create("http://localhost:" + wireMock.getHttpPort()))
                        .region(Region.US_EAST_1)
                        .credentialsProvider(
                            StaticCredentialsProvider.create(AwsBasicCredentials.create("akid", "skid")));
            Track1SdkAutoHook.beforeBuild(builder);
            Track1SdkAutoHook.beforeBuild(builder);
            assertThat(builder.overrideConfiguration().executionInterceptors()).hasSize(1);
            try (S3Client built = (S3Client) Track1SdkAutoHook.wrapClient(builder.build())) {
                assertThat(built).isInstanceOf(PassthroughS3Clients.Handle.class);
            }
        } finally {
            if (prev == null) {
                System.clearProperty("track1.sdk.auto");
            } else {
                System.setProperty("track1.sdk.auto", prev);
            }
        }
    }

    /**
     * {@code whenComplete} is a dependent action on the delegate future. P0
     * returns that future unchanged so cancel stays on the AWS object; the
     * probe line may therefore land a tick after {@code join()}.
     */
    private static void awaitProbe(BooleanSupplier ready) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!ready.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("probe record did not arrive");
            }
            Thread.sleep(10);
        }
    }

    private static void stubRange(String range, int status, String body) {
        stubFor(get(urlPathEqualTo("/bucket/key"))
                    .withHeader("Range", equalTo(range))
                    .willReturn(aResponse().withStatus(status).withBody(body)));
    }

    private static byte[] readAll(InputStream in) throws Exception {
        byte[] buf = new byte[32];
        int n = in.read(buf);
        if (n <= 0) {
            return new byte[0];
        }
        byte[] out = new byte[n];
        System.arraycopy(buf, 0, out, 0, n);
        return out;
    }
}
