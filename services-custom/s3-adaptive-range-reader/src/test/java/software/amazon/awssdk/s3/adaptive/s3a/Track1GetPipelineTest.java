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
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.s3.adaptive.internal.RuntimeConfig;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

@WireMockTest
class Track1GetPipelineTest {

    private S3Client rawSync;
    private S3AsyncClient rawAsync;
    private Track1S3aRuntime runtime;

    @BeforeEach
    void setUp(WireMockRuntimeInfo wireMock) {
        Track1S3aProbe.shared().reset();
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
        if (runtime != null) {
            runtime.close();
        }
        if (rawSync != null) {
            rawSync.close();
        }
        if (rawAsync != null) {
            rawAsync.close();
        }
        Track1S3aProbe.shared().reset();
        Track1S3aRuntime.resetShared(RuntimeConfig.builder().build());
    }

    @Test
    void d1CoveringHitAvoidsSecondGet() throws Exception {
        stubBody("0123456789");
        runtime = newRuntime(RuntimeConfig.builder().d1Enabled(true).build());
        S3Client wrapped = wrap();
        assertThat(read(wrapped, 0, 9)).isEqualTo("0123456789".getBytes());
        WireMock.reset();
        stubFor(get(urlPathEqualTo("/bucket/key"))
                    .willReturn(aResponse().withStatus(500).withBody("should-not-run")));
        assertThat(read(wrapped, 2, 6)).isEqualTo("23456".getBytes());
        assertThat(runtime.stats().cacheHits()).isEqualTo(1);
        assertThat(runtime.stats().remoteGets()).isEqualTo(1);
        assertThat(runtime.stats().teedGets()).isEqualTo(1);
    }

    @Test
    void d1RejectsLargeRangeAndDoesNotCache() throws Exception {
        stubBody("0123456789abcdefghij");
        runtime = newRuntime(RuntimeConfig.builder()
                                          .d1Enabled(true)
                                          .d1AdmitMaxBytes(8)
                                          .build());
        S3Client wrapped = wrap();
        assertThat(read(wrapped, 0, 19)).isEqualTo("0123456789abcdefghij".getBytes());
        assertThat(read(wrapped, 0, 19)).isEqualTo("0123456789abcdefghij".getBytes());
        assertThat(runtime.stats().admitRejected()).isEqualTo(2);
        assertThat(runtime.stats().cacheHits()).isZero();
        assertThat(runtime.stats().cacheMisses()).isZero();
        assertThat(runtime.stats().teedGets()).isZero();
        verify(2, getRequestedFor(urlPathEqualTo("/bucket/key")));
    }

    @Test
    void adaptiveObserveUsesPolicyAdmissionInsteadOfStaticAdmitMax() throws Exception {
        stubBody("0123456789abcdefghij");
        runtime = newRuntime(RuntimeConfig.builder()
                                          .d1Enabled(true)
                                          .d1Adaptive(true)
                                          .d1AdmitMaxBytes(8)
                                          .d1ObserveGets(1000)
                                          .build());
        S3Client wrapped = wrap();
        assertThat(read(wrapped, 0, 19)).isEqualTo("0123456789abcdefghij".getBytes());
        assertThat(runtime.stats().admitRejected()).isZero();
        assertThat(runtime.stats().teedGets()).isZero();
        assertThat(runtime.controller().observations()).isEqualTo(1);
        assertThat(runtime.controller().mode()).isEqualTo(D1SoftController.Mode.OBSERVE);
        assertThat(read(wrapped, 0, 19)).isEqualTo("0123456789abcdefghij".getBytes());
        assertThat(runtime.stats().teedGets()).isEqualTo(1);
    }

    @Test
    void d1TeesAdmittedRangeThenServesCoveringHit() throws Exception {
        stubBody("0123456789abcdefghij");
        runtime = newRuntime(RuntimeConfig.builder()
                                          .d1Enabled(true)
                                          .d1AdmitMaxBytes(32)
                                          .build());
        S3Client wrapped = wrap();
        assertThat(read(wrapped, 0, 19)).isEqualTo("0123456789abcdefghij".getBytes());
        WireMock.reset();
        stubFor(get(urlPathEqualTo("/bucket/key"))
                    .willReturn(aResponse().withStatus(500).withBody("should-not-run")));
        assertThat(read(wrapped, 4, 8)).isEqualTo("45678".getBytes());
        assertThat(runtime.stats().teedGets()).isEqualTo(1);
        assertThat(runtime.stats().cacheHits()).isEqualTo(1);
        assertThat(runtime.stats().admitRejected()).isZero();
    }

    @Test
    void d2ReturnsExactBytes() throws Exception {
        stubBody("abcdefghij");
        runtime = newRuntime(RuntimeConfig.builder().d2Enabled(true).d2WaitWindowMicros(0).build());
        runtime.registerAsync(rawAsync);
        assertThat(read(wrap(), 0, 9)).isEqualTo("abcdefghij".getBytes());
        assertThat(runtime.stats().remoteGets()).isEqualTo(1);
        assertThat(runtime.stats().mergedGets()).isZero();
    }

    @Test
    void d2WithoutAsyncClientPreservesStreamingSyncPassthrough() throws Exception {
        stubBody("abcdefghij");
        runtime = newRuntime(RuntimeConfig.builder().d2Enabled(true).build());
        assertThat(read(wrap(), 0, 9)).isEqualTo("abcdefghij".getBytes());
        assertThat(runtime.stats().queueSubmissions()).isZero();
        assertThat(runtime.stats().remoteGets()).isZero();
    }

    @Test
    void d4MergesNeighbourRangesIntoOneGet() throws Exception {
        stubBody("0123456789abcdefghij");
        runtime = newRuntime(RuntimeConfig.builder()
                                          .d2Enabled(true)
                                          .d4Enabled(true)
                                          .d2WaitWindowMicros(50_000)
                                          .d4MaxWasteRatio(0.8)
                                          .maxSingleFetchBytes(1024)
                                          .maxConcurrentGets(16)
                                          .build());
        runtime.registerAsync(rawAsync);
        final S3Client wrapped = wrap();
        final CountDownLatch start = new CountDownLatch(1);
        final AtomicReference<byte[]> a = new AtomicReference<byte[]>();
        final AtomicReference<byte[]> b = new AtomicReference<byte[]>();
        Thread t1 = new Thread(new Runnable() {
            @Override
            public void run() {
                await(start);
                a.set(readQuiet(wrapped, 0, 4));
            }
        });
        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                await(start);
                b.set(readQuiet(wrapped, 10, 14));
            }
        });
        t1.start();
        t2.start();
        start.countDown();
        t1.join(5_000);
        t2.join(5_000);
        assertThat(a.get()).isEqualTo("01234".getBytes());
        assertThat(b.get()).isEqualTo("abcde".getBytes());
        verify(1, getRequestedFor(urlPathEqualTo("/bucket/key")));
        assertThat(runtime.stats().mergedGets()).isEqualTo(1);
        assertThat(runtime.stats().queueSubmissions()).isEqualTo(2);
        assertThat(runtime.stats().sameObjectMultiTicketGroups()).isEqualTo(1);
        assertThat(runtime.stats().mergeableGroups()).isEqualTo(1);
        assertThat(runtime.stats().queueWaitNanos()).isPositive();
    }

    @Test
    void serviceErrorIsNotSwallowed() {
        stubFor(get(urlPathEqualTo("/bucket/missing"))
                    .willReturn(aResponse().withStatus(404)
                                           .withBody("<Error><Code>NoSuchKey</Code></Error>")));
        runtime = newRuntime(RuntimeConfig.builder().d1Enabled(true).build());
        GetObjectRequest req = GetObjectRequest.builder().bucket("bucket").key("missing")
                                               .range("bytes=0-3").build();
        assertThatThrownBy(() -> wrap().getObject(req)).isInstanceOf(S3Exception.class);
        assertThat(runtime.stats().fallbacks()).isZero();
    }

    @Test
    void queueFaultFallsBackToExactSyncGet() throws Exception {
        stubBody("xyz");
        runtime = newRuntime(RuntimeConfig.builder().d2Enabled(true).build());
        runtime.registerAsync(rawAsync);
        runtime.queue().close();
        assertThat(read(wrap(), 0, 2)).isEqualTo("xyz".getBytes());
        assertThat(runtime.stats().fallbacks()).isEqualTo(1);
    }

    private Track1S3aRuntime newRuntime(RuntimeConfig config) {
        return new Track1S3aRuntime(config);
    }

    private S3Client wrap() {
        return PassthroughS3Clients.wrapSync(rawSync, Track1S3aProbe.shared(), runtime.pipeline());
    }

    private static void stubBody(String body) {
        stubFor(get(urlPathEqualTo("/bucket/key"))
                    .willReturn(aResponse().withStatus(206).withBody(body)));
    }

    private static byte[] read(S3Client client, long start, long end) throws Exception {
        GetObjectRequest req = GetObjectRequest.builder()
                                               .bucket("bucket")
                                               .key("key")
                                               .range("bytes=" + start + "-" + end)
                                               .build();
        try (ResponseInputStream<GetObjectResponse> in = client.getObject(req)) {
            return readAll(in);
        }
    }

    private static byte[] readQuiet(S3Client client, long start, long end) {
        try {
            return read(client, start, end);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static byte[] readAll(InputStream in) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) >= 0) {
            if (n > 0) {
                out.write(buf, 0, n);
            }
        }
        return out.toByteArray();
    }
}
