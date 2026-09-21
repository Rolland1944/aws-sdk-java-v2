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

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.DefaultS3ClientFactory;
import org.apache.hadoop.fs.s3a.S3ClientFactory;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

/**
 * S3A client factory that inserts Track 1 in front of every AWS client S3A
 * constructs. Installation is a single Hadoop key:
 *
 * <pre>
 *   fs.s3a.s3.client.factory.impl =
 *     software.amazon.awssdk.s3.adaptive.s3a.Track1S3ClientFactory
 * </pre>
 *
 * <p>S3A is not modified. The factory constructs the same clients
 * {@link DefaultS3ClientFactory} would, then wraps {@code GetObject}. With
 * every dimension off this is still P0 passthrough. D1/D2/D4 attach to the
 * sync GET arm only and default off.
 */
@SdkPublicApi
public final class Track1S3ClientFactory implements S3ClientFactory, Configurable {

    private final DefaultS3ClientFactory delegate = new DefaultS3ClientFactory();
    private final Track1S3aProbe probe = Track1S3aProbe.shared();
    private final Track1S3aRuntime runtime = Track1S3aRuntime.shared();

    @Override
    public void setConf(Configuration conf) {
        delegate.setConf(conf);
    }

    @Override
    public Configuration getConf() {
        return delegate.getConf();
    }

    @Override
    public S3Client createS3Client(URI uri, S3ClientCreationParameters parameters) throws IOException {
        S3Client raw = delegate.createS3Client(uri, parameters);
        runtime.registerSync(raw);
        ensureTrack1AsyncClient(uri, parameters);
        return PassthroughS3Clients.wrapSync(raw, probe, runtime.pipeline());
    }

    @Override
    public S3AsyncClient createS3AsyncClient(URI uri, S3ClientCreationParameters parameters)
            throws IOException {
        S3AsyncClient raw = delegate.createS3AsyncClient(uri, parameters);
        runtime.registerAsync(raw);
        return PassthroughS3Clients.wrapAsync(raw, probe);
    }

    @Override
    public S3TransferManager createS3TransferManager(S3AsyncClient s3AsyncClient) {
        // Transfer manager talks to the raw AWS client. Feeding it the proxy
        // would couple CRT/transfer internals to our invocation handler.
        return delegate.createS3TransferManager(PassthroughS3Clients.unwrapAsync(s3AsyncClient));
    }

    private void ensureTrack1AsyncClient(URI uri, S3ClientCreationParameters parameters) {
        if (!runtime.config().d2Enabled() || runtime.asyncClient() != null) {
            return;
        }
        S3AsyncClient created = null;
        try {
            created = delegate.createS3AsyncClient(uri, parameters);
            if (!runtime.registerOwnedAsync(created)) {
                created.close();
            }
        } catch (IOException | RuntimeException ignored) {
            if (created != null) {
                try {
                    created.close();
                } catch (Throwable closeIgnored) {
                    // D2 bootstrap is best-effort; sync passthrough remains valid.
                }
            }
        }
    }
}
