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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.InterceptorContext;
import software.amazon.awssdk.core.internal.interceptor.DefaultFailedExecutionContext;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.http.SdkHttpRequest;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

class Track2IoCollectorInterceptorTest {

    private static final String REFERER =
        "https://audit.example.org/hadoop/1/op_open/3c0d9b7e-2a63-43d9-a220-3c574d768ef3-3/"
        + "?op=op_open&p1=s3a%3A%2F%2Fbench%2Ftpch%2Flineitem%2Fpart-0.parquet"
        + "&pr=hadoop&ps=235865a0-d399-4696-9978-64568db1b51c"
        + "&id=3c0d9b7e-2a63-43d9-a220-3c574d768ef3-3&t0=12&t1=44&ts=1617116985923&sqlid=17";

    // ------------------------------------------------------------ range parsing

    @Test
    void parseRange_readsInclusiveEndAsLength() {
        assertThat(Track2IoCollectorInterceptor.parseRange("bytes=0-99")).containsExactly(0L, 100L);
        assertThat(Track2IoCollectorInterceptor.parseRange("bytes=1048576-1048576")).containsExactly(1048576L, 1L);
    }

    @Test
    void parseRange_openEndedRangeHasUnknownLength() {
        assertThat(Track2IoCollectorInterceptor.parseRange("bytes=4096-")).containsExactly(4096L, -1L);
    }

    @Test
    void parseRange_rejectsWhatWeCannotAttribute() {
        assertThat(Track2IoCollectorInterceptor.parseRange(null)).isNull();
        assertThat(Track2IoCollectorInterceptor.parseRange("items=0-99")).isNull();
        assertThat(Track2IoCollectorInterceptor.parseRange("bytes=0-99,200-299")).isNull();
        assertThat(Track2IoCollectorInterceptor.parseRange("bytes=-99")).isNull();
        assertThat(Track2IoCollectorInterceptor.parseRange("bytes=99-0")).isNull();
        assertThat(Track2IoCollectorInterceptor.parseRange("bytes=abc-def")).isNull();
    }

    // ---------------------------------------------------------- referer parsing

    @Test
    void parseRefererQuery_decodesAuditFields() {
        Map<String, String> fields = Track2IoCollectorInterceptor.parseRefererQuery(REFERER);
        assertThat(fields).containsEntry("op", "op_open")
                          .containsEntry("p1", "s3a://bench/tpch/lineitem/part-0.parquet")
                          .containsEntry("id", "3c0d9b7e-2a63-43d9-a220-3c574d768ef3-3")
                          .containsEntry("t1", "44")
                          .containsEntry("sqlid", "17");
    }

    @Test
    void parseRefererQuery_toleratesTruncatedOrAbsentHeader() {
        // S3A drops the query when the header grows too long; the span id survives in the path
        assertThat(Track2IoCollectorInterceptor.parseRefererQuery(null)).isEmpty();
        assertThat(Track2IoCollectorInterceptor.parseRefererQuery("https://audit.example.org/hadoop/1/op_open/x/"))
            .isEmpty();
        assertThat(Track2IoCollectorInterceptor.parseRefererQuery("https://a/?op=op_open&trailing"))
            .containsEntry("op", "op_open")
            .containsEntry("trailing", "");
    }

    // -------------------------------------------------------------- serialising

    @Test
    void toJson_omitsNullsAndQuotesOnlyNonNumbers() {
        Map<String, Object> rec = new LinkedHashMap<>();
        rec.put("a", 1L);
        rec.put("b", null);
        rec.put("c", "x");
        assertThat(Track2IoCollectorInterceptor.toJson(rec)).isEqualTo("{\"a\":1,\"c\":\"x\"}");
    }

    @Test
    void escape_keepsRecordsOnOneLine() {
        assertThat(Track2IoCollectorInterceptor.escape("a\"b\\c\nd\te")).isEqualTo("a\\\"b\\\\c\\nd\\te");
        assertThat(Track2IoCollectorInterceptor.escape("\u0001")).isEqualTo("\\u0001");
    }

    // ---------------------------------------------------------------- full flow

    @Test
    void successfulGet_recordsRangeLatencyAndCorrelationKey() {
        StringWriter out = new StringWriter();
        Track2IoCollectorInterceptor interceptor = newInterceptor(out);
        ExecutionAttributes attributes = new ExecutionAttributes();

        InterceptorContext context = contextFor(getRequest("bytes=1024-2047", REFERER), response(206, "1024"));
        interceptor.beforeExecution(context, attributes);
        interceptor.beforeTransmission(context, attributes);
        interceptor.afterTransmission(context, attributes);

        String line = out.toString().trim();
        assertThat(line).contains("\"range_offset\":1024")
                        .contains("\"range_length\":1024")
                        .contains("\"http_status\":206")
                        .contains("\"bytes_expected\":1024")
                        .contains("\"attempt\":0")
                        .contains("\"audit_op\":\"op_open\"")
                        .contains("\"audit_sqlid\":\"17\"")
                        .contains("\"audit_path\":\"s3a://bench/tpch/lineitem/part-0.parquet\"");
        assertThat(line).containsPattern("\"latency_ns\":\\d+");
        assertThat(line.split("\n")).hasSize(1);
    }

    @Test
    void retriedGet_numbersAttemptsWithinOneExecution() {
        StringWriter out = new StringWriter();
        Track2IoCollectorInterceptor interceptor = newInterceptor(out);
        ExecutionAttributes attributes = new ExecutionAttributes();

        InterceptorContext failed = contextFor(getRequest("bytes=0-99", REFERER), response(500, null));
        InterceptorContext ok = contextFor(getRequest("bytes=0-99", REFERER), response(206, "100"));
        interceptor.beforeExecution(failed, attributes);
        interceptor.beforeTransmission(failed, attributes);
        interceptor.afterTransmission(failed, attributes);
        interceptor.beforeTransmission(ok, attributes);
        interceptor.afterTransmission(ok, attributes);

        String[] lines = out.toString().trim().split("\n");
        assertThat(lines).hasSize(2);
        assertThat(lines[0]).contains("\"attempt\":0").contains("\"http_status\":500");
        assertThat(lines[1]).contains("\"attempt\":1").contains("\"http_status\":206");
    }

    /**
     * A transmission that never completes skips {@code afterTransmission} entirely. Without draining the outstanding
     * count on failure the JVM-wide gauge would climb on every retry, so this asserts the balance directly rather than
     * the record contents.
     */
    @Test
    void failedExecution_leavesInflightGaugeBalanced() {
        StringWriter out = new StringWriter();
        Track2IoCollectorInterceptor interceptor = newInterceptor(out);
        ExecutionAttributes attributes = new ExecutionAttributes();
        int before = Track2IoCollectorInterceptor.inflight();

        InterceptorContext context = contextFor(getRequest("bytes=0-99", REFERER), null);
        interceptor.beforeExecution(context, attributes);
        interceptor.beforeTransmission(context, attributes);
        interceptor.onExecutionFailure(DefaultFailedExecutionContext.builder()
                                                                    .interceptorContext(context)
                                                                    .exception(new IOException("connection reset"))
                                                                    .build(),
                                       attributes);

        assertThat(Track2IoCollectorInterceptor.inflight()).isEqualTo(before);
        assertThat(out.toString()).contains("\"error\":\"java.io.IOException\"");
    }

    @Test
    void requestWithoutRangeIsStillRecorded() {
        StringWriter out = new StringWriter();
        Track2IoCollectorInterceptor interceptor = newInterceptor(out);
        ExecutionAttributes attributes = new ExecutionAttributes();

        InterceptorContext context = contextFor(getRequest(null, null), response(200, "42"));
        interceptor.beforeExecution(context, attributes);
        interceptor.beforeTransmission(context, attributes);
        interceptor.afterTransmission(context, attributes);

        String line = out.toString().trim();
        assertThat(line).contains("\"http_status\":200")
                        .doesNotContain("range_offset")
                        .doesNotContain("audit_op");
    }

    // ------------------------------------------------------------------ helpers

    private static Track2IoCollectorInterceptor newInterceptor(StringWriter out) {
        return new Track2IoCollectorInterceptor(new Track2IoCollectorInterceptor.RecordSink(out),
                                                Arrays.asList("sqlid", "ji", "ta"));
    }

    private static SdkHttpRequest getRequest(String range, String referer) {
        SdkHttpRequest.Builder builder = SdkHttpRequest.builder()
                                                       .method(SdkHttpMethod.GET)
                                                       .protocol("https")
                                                       .host("bench.s3.us-east-1.amazonaws.com")
                                                       .encodedPath("/tpch/lineitem/part-0.parquet");
        if (range != null) {
            builder.putHeader("Range", range);
        }
        if (referer != null) {
            builder.putHeader("Referer", referer);
        }
        return builder.build();
    }

    private static SdkHttpResponse response(int status, String contentLength) {
        SdkHttpResponse.Builder builder = SdkHttpResponse.builder().statusCode(status);
        if (contentLength != null) {
            builder.putHeader("Content-Length", contentLength);
        }
        return builder.build();
    }

    private static InterceptorContext contextFor(SdkHttpRequest httpRequest, SdkHttpResponse httpResponse) {
        InterceptorContext.Builder builder =
            InterceptorContext.builder()
                              .request(GetObjectRequest.builder().bucket("bench").key("k").build())
                              .httpRequest(httpRequest);
        if (httpResponse != null) {
            builder.httpResponse(httpResponse);
        }
        return builder.build();
    }
}
