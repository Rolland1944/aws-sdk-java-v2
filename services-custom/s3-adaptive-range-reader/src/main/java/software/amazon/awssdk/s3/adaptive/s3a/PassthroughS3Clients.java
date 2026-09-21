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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import software.amazon.awssdk.annotations.SdkInternalApi;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * JDK proxies in front of S3A's AWS clients.
 *
 * <p>Non-{@code getObject} methods always go to the delegate. Async
 * {@code getObject} stays passthrough (P0: Spark/S3A's path is sync). Sync
 * {@code getObject} goes through {@link Track1GetPipeline} when any of D1/D2/D4
 * is on; otherwise it is still the delegate's object. Pipeline faults fall
 * back to the original exact-range GET.
 */
@SdkInternalApi
public final class PassthroughS3Clients {

    private PassthroughS3Clients() {
    }

    public static S3Client wrapSync(S3Client delegate, Track1S3aProbe probe) {
        return wrapSync(delegate, probe, Track1S3aRuntime.shared().pipeline());
    }

    public static S3Client wrapSync(S3Client delegate, Track1S3aProbe probe, Track1GetPipeline pipeline) {
        if (delegate == null) {
            return null;
        }
        if (delegate instanceof Handle && ((Handle) delegate).clientKind().equals("sync")) {
            return delegate;
        }
        return (S3Client) Proxy.newProxyInstance(
            S3Client.class.getClassLoader(),
            new Class<?>[] { S3Client.class, Handle.class },
            new Handler(delegate, "sync", probe, pipeline));
    }

    public static S3AsyncClient wrapAsync(S3AsyncClient delegate, Track1S3aProbe probe) {
        if (delegate == null) {
            return null;
        }
        if (delegate instanceof Handle && ((Handle) delegate).clientKind().equals("async")) {
            return delegate;
        }
        return (S3AsyncClient) Proxy.newProxyInstance(
            S3AsyncClient.class.getClassLoader(),
            new Class<?>[] { S3AsyncClient.class, Handle.class },
            new Handler(delegate, "async", probe, null));
    }

    public static S3Client unwrapSync(S3Client client) {
        return client instanceof Handle ? (S3Client) ((Handle) client).delegate() : client;
    }

    public static S3AsyncClient unwrapAsync(S3AsyncClient client) {
        return client instanceof Handle ? (S3AsyncClient) ((Handle) client).delegate() : client;
    }

    /** Marker implemented by the proxy so unwrap / double-wrap are O(1). */
    public interface Handle {
        Object delegate();

        String clientKind();
    }

    private static final class Handler implements InvocationHandler {
        private final Object delegate;
        private final String clientKind;
        private final Track1S3aProbe probe;
        private final Track1GetPipeline pipeline;

        Handler(Object delegate, String clientKind, Track1S3aProbe probe, Track1GetPipeline pipeline) {
            this.delegate = delegate;
            this.clientKind = clientKind;
            this.probe = probe;
            this.pipeline = pipeline;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.getDeclaringClass() == Handle.class) {
                if ("delegate".equals(method.getName())) {
                    return delegate;
                }
                return clientKind;
            }
            if (method.getDeclaringClass() == Object.class) {
                return invokeObject(proxy, method, args);
            }
            if (!"getObject".equals(method.getName())) {
                return invokeDelegate(method, args);
            }
            return invokeGet(method, args);
        }

        private Object invokeGet(Method method, Object[] args) throws Throwable {
            GetObjectRequest request = findRequest(args);
            long start = System.nanoTime();
            if (pipeline != null && pipeline.enabled()
                && ResponseInputStream.class.isAssignableFrom(method.getReturnType())) {
                try {
                    ResponseInputStream<GetObjectResponse> intercepted =
                        pipeline.trySyncGet((S3Client) delegate, request);
                    if (intercepted != null) {
                        probe.record(clientKind, request, start, null);
                        return intercepted;
                    }
                } catch (Throwable t) {
                    probe.record(clientKind, request, start, t);
                    throw t;
                }
            }
            try {
                Object result = invokeDelegate(method, args);
                if (result instanceof CompletableFuture) {
                    CompletableFuture<?> future = (CompletableFuture<?>) result;
                    future.whenComplete((ok, err) -> {
                        Throwable cause = unwrap(err);
                        probe.record(clientKind, request, start, cause);
                    });
                    return result;
                }
                probe.record(clientKind, request, start, null);
                return result;
            } catch (Throwable t) {
                probe.record(clientKind, request, start, t);
                throw t;
            }
        }

        private Object invokeDelegate(Method method, Object[] args) throws Throwable {
            try {
                return method.invoke(delegate, args);
            } catch (InvocationTargetException e) {
                Throwable cause = e.getCause();
                throw cause == null ? e : cause;
            }
        }

        private Object invokeObject(Object proxy, Method method, Object[] args) {
            String name = method.getName();
            if ("hashCode".equals(name)) {
                return System.identityHashCode(proxy);
            }
            if ("equals".equals(name)) {
                return proxy == args[0];
            }
            if ("toString".equals(name)) {
                return "Track1Passthrough(" + clientKind + "," + delegate + ")";
            }
            try {
                return method.invoke(delegate, args);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        private static GetObjectRequest findRequest(Object[] args) {
            if (args == null) {
                return null;
            }
            for (Object arg : args) {
                if (arg instanceof GetObjectRequest) {
                    return (GetObjectRequest) arg;
                }
            }
            return null;
        }

        private static Throwable unwrap(Throwable err) {
            if (err instanceof CompletionException && err.getCause() != null) {
                return err.getCause();
            }
            return err;
        }
    }
}
