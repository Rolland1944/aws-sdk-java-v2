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

import java.lang.instrument.Instrumentation;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.bytecode.assign.Assigner;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.utility.JavaModule;
import software.amazon.awssdk.annotations.SdkPublicApi;

/**
 * Lightweight Java agent that transparently attaches RECAS Track 1 + Track 2 to any
 * {@code S3Client.builder().build()} call across any plugin classloader (e.g., Trino
 * {@code plugin/hive/}) when {@code -Dtrack1.sdk.auto=true} is set.
 */
@SdkPublicApi
public final class Track1SdkAgent {

    private Track1SdkAgent() {
    }

    public static void premain(String agentArgs, Instrumentation inst) {
        install(inst);
    }

    public static void agentmain(String agentArgs, Instrumentation inst) {
        install(inst);
    }

    private static void install(Instrumentation inst) {
        new AgentBuilder.Default()
            .disableClassFormatChanges()
            .with(AgentBuilder.RedefinitionStrategy.RETRANSFORMATION)
            .ignore(ElementMatchers.nameStartsWith("net.bytebuddy.")
                .or(ElementMatchers.nameStartsWith("java."))
                .or(ElementMatchers.nameStartsWith("jdk."))
                .or(ElementMatchers.nameStartsWith("sun.")))
            .type(ElementMatchers.named("software.amazon.awssdk.services.s3.DefaultS3ClientBuilder")
                .or(ElementMatchers.named("software.amazon.awssdk.services.s3.DefaultS3AsyncClientBuilder")))
            .transform(new AgentBuilder.Transformer() {
                @Override
                public DynamicType.Builder<?> transform(DynamicType.Builder<?> builder,
                                                        TypeDescription typeDescription,
                                                        ClassLoader classLoader,
                                                        JavaModule module,
                                                        java.security.ProtectionDomain protectionDomain) {
                    return builder.visit(
                        Advice.to(BuildClientAdvice.class)
                              .on(ElementMatchers.named("buildClient")
                                  .and(ElementMatchers.not(ElementMatchers.isBridge()))
                                  .and(ElementMatchers.takesArguments(0))));
                }
            })
            .installOn(inst);
    }

    public static final class BuildClientAdvice {

        private BuildClientAdvice() {
        }

        @Advice.OnMethodEnter
        public static void onEnter(@Advice.This Object builder) {
            if (!Boolean.getBoolean("track1.sdk.auto") || builder == null) {
                return;
            }
            try {
                Class<?> hook = Class.forName(
                    "software.amazon.awssdk.s3.adaptive.s3a.Track1SdkAutoHook",
                    true,
                    builder.getClass().getClassLoader());
                hook.getMethod("beforeBuild", Object.class).invoke(null, builder);
            } catch (Throwable ignored) {
                // Keep S3Client construction unaffected.
            }
        }

        @Advice.OnMethodExit
        public static void onExit(
            @Advice.This Object builder,
            @Advice.Return(readOnly = false, typing = Assigner.Typing.DYNAMIC) Object returned
        ) {
            if (!Boolean.getBoolean("track1.sdk.auto") || returned == null || builder == null) {
                return;
            }
            try {
                Class<?> hook = Class.forName(
                    "software.amazon.awssdk.s3.adaptive.s3a.Track1SdkAutoHook",
                    true,
                    builder.getClass().getClassLoader());
                returned = hook.getMethod("wrapClient", Object.class).invoke(null, returned);
            } catch (Throwable ignored) {
                // Keep unwrapped client if hook class is absent in this classloader.
            }
        }
    }
}
