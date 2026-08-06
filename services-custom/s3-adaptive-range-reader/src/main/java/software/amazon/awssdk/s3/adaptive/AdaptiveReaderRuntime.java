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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import software.amazon.awssdk.annotations.SdkPublicApi;
import software.amazon.awssdk.s3.adaptive.internal.AdaptivePolicySelector;
import software.amazon.awssdk.s3.adaptive.internal.DecisionTreePolicySelector;
import software.amazon.awssdk.s3.adaptive.internal.FixedPolicySelector;
import software.amazon.awssdk.s3.adaptive.internal.LockingPolicySelector;
import software.amazon.awssdk.s3.adaptive.internal.PrefixPolicySelector;
import software.amazon.awssdk.s3.adaptive.internal.RuleBasedPolicySelector;
import software.amazon.awssdk.s3.adaptive.internal.RuntimeConfig;
import software.amazon.awssdk.s3.adaptive.internal.SharedPolicySelector;
import software.amazon.awssdk.s3.adaptive.internal.budget.AppBudgetLease;
import software.amazon.awssdk.s3.adaptive.internal.budget.ConcurrencyLimiter;
import software.amazon.awssdk.s3.adaptive.internal.budget.GlobalBudget;
import software.amazon.awssdk.s3.adaptive.internal.budget.InflightLimiter;
import software.amazon.awssdk.s3.adaptive.internal.cache.AppCache;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.utils.SdkAutoCloseable;

/**
 * The process-level entry point for Track 1 S3. A single runtime is shared by every query engine in the JVM: it owns
 * the global work-conserving cache {@link GlobalBudget}, the process-wide concurrent-GET limiter, and the immutable
 * decision-tree model. Each engine {@link #register(String) registers} once to obtain an isolated {@link AppContext}
 * (its own cache/budget-lease/in-flight cap/cross-object selector). Assigning a distinct appID per engine is how
 * environments are isolated: Flink and Spark sharing one JVM get separate caches, budgets, metrics, and feature
 * windows while still respecting one global ceiling.
 *
 * <p>Enabling prefetch (depth &gt; 0) turns on speculative look-ahead; {@code prefetchDepth(0)} makes readers behave
 * like the synchronous S2 reader (still per-app cache + cross-object selector). This is experimental and opt-in.
 */
@SdkPublicApi
public final class AdaptiveReaderRuntime implements SdkAutoCloseable {

    private final RuntimeConfig config;
    private final GlobalBudget budget;
    private final ConcurrencyLimiter concurrency;
    private final DecisionTreePolicySelector model;
    private final SelectorMode selectorMode;
    private final PolicyName fixedPolicy;
    private final Map<String, PolicyName> prefixPolicies;
    private final S3AsyncClient s3Async;
    private final Map<String, AppContext> apps = new ConcurrentHashMap<>();
    private final AtomicInteger sequence = new AtomicInteger();

    private AdaptiveReaderRuntime(Builder b) {
        this.config = b.configBuilder.build();
        this.budget = new GlobalBudget(config.globalCacheBytes());
        this.concurrency = new ConcurrencyLimiter(config.maxConcurrentGets());
        this.model = b.model != null ? b.model : DecisionTreePolicySelector.fromDefaultResource();
        this.selectorMode = b.selectorMode;
        this.fixedPolicy = b.fixedPolicy;
        this.prefixPolicies = b.prefixPolicies;
        this.s3Async = b.s3Async;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Register a query engine and allocate its isolated environment. The returned {@link AppContext} carries a unique
     * appID derived from {@code engineName}; call once per engine and reuse the context for all of that engine's
     * readers.
     */
    public AppContext register(String engineName) {
        String appId = engineName + "-" + sequence.incrementAndGet();
        AppBudgetLease lease = new AppBudgetLease(budget, appId);
        AppCache cache = new AppCache(lease);
        budget.register(appId, config.perAppReservedBytes(), cache);
        InflightLimiter inflight = new InflightLimiter(config.perAppInflightBytes());
        SharedPolicySelector selector = newSelector();
        AppContext ctx = new AppContext(appId, config, lease, cache, concurrency, inflight, selector, s3Async);
        apps.put(appId, ctx);
        return ctx;
    }

    private SharedPolicySelector newSelector() {
        if (fixedPolicy != null) {
            return new FixedPolicySelector(fixedPolicy);
        }
        if (prefixPolicies != null) {
            return new PrefixPolicySelector(prefixPolicies);
        }
        return selectorMode == SelectorMode.TEMPLATE_AUTO
               ? new RuleBasedPolicySelector()
               : new LockingPolicySelector(new AdaptivePolicySelector(model));
    }

    /**
     * A process-level snapshot including a per-app breakdown (used to verify isolation).
     */
    public RuntimeMetrics metrics() {
        Map<String, AppMetrics> perApp = new LinkedHashMap<>();
        for (Map.Entry<String, AppContext> entry : apps.entrySet()) {
            perApp.put(entry.getKey(), entry.getValue().metrics());
        }
        return new RuntimeMetrics(budget.capacity(), budget.totalUsage(), concurrency.maxConcurrent(),
                                  concurrency.peak(), perApp);
    }

    @Override
    public void close() {
        // Readers and the S3AsyncClient are owned by the caller; nothing process-owned to release here.
        apps.clear();
    }

    /**
     * Builder for {@link AdaptiveReaderRuntime}.
     */
    @SdkPublicApi
    public static final class Builder {

        private final RuntimeConfig.Builder configBuilder = RuntimeConfig.builder();
        private S3AsyncClient s3Async;
        private DecisionTreePolicySelector model;
        private SelectorMode selectorMode = SelectorMode.DECISION_TREE;
        private PolicyName fixedPolicy;
        private Map<String, PolicyName> prefixPolicies;

        private Builder() {
        }

        /**
         * Which policy brain routes reads to executors: the learned {@link SelectorMode#DECISION_TREE} (default) or the
         * hand-rule {@link SelectorMode#TEMPLATE_AUTO} comparison baseline.
         */
        public Builder selectorMode(SelectorMode selectorMode) {
            this.selectorMode = selectorMode != null ? selectorMode : SelectorMode.DECISION_TREE;
            return this;
        }

        /**
         * Route every read to one fixed policy. This is intended for
         * controlled comparisons against the adaptive selector.
         */
        public Builder forcePolicy(PolicyName forcePolicy) {
            this.fixedPolicy = forcePolicy;
            return this;
        }

        Builder prefixPolicyMap(Map<String, PolicyName> prefixPolicies) {
            if (prefixPolicies == null || prefixPolicies.isEmpty()) {
                throw new IllegalArgumentException("prefixPolicies must not be empty");
            }
            this.prefixPolicies = new LinkedHashMap<>(prefixPolicies);
            return this;
        }

        /**
         * The async S3 client used by {@link AppContext#newReader(String, String)}. Optional if callers always supply
         * their own {@code AsyncObjectStore}. The client's lifecycle is owned by the caller.
         */
        public Builder s3AsyncClient(S3AsyncClient s3Async) {
            this.s3Async = s3Async;
            return this;
        }

        /**
         * Global cache byte ceiling shared by all apps.
         */
        public Builder globalCacheBytes(long globalCacheBytes) {
            configBuilder.globalCacheBytes(globalCacheBytes);
            return this;
        }

        /**
         * Guaranteed cache floor each app can always use (best-effort if apps oversubscribe the global ceiling).
         */
        public Builder perAppReservedBytes(long perAppReservedBytes) {
            configBuilder.perAppReservedBytes(perAppReservedBytes);
            return this;
        }

        public Builder maxConcurrentGets(int maxConcurrentGets) {
            configBuilder.maxConcurrentGets(maxConcurrentGets);
            return this;
        }

        public Builder perAppInflightBytes(long perAppInflightBytes) {
            configBuilder.perAppInflightBytes(perAppInflightBytes);
            return this;
        }

        /**
         * Speculative look-ahead depth (blocks). {@code 0} disables prefetch (S2-equivalent behaviour).
         */
        public Builder prefetchDepth(int prefetchDepth) {
            configBuilder.prefetchDepth(prefetchDepth);
            return this;
        }

        public Builder prefetchBlockSize(long prefetchBlockSize) {
            configBuilder.prefetchBlockSize(prefetchBlockSize);
            return this;
        }

        public Builder maxSingleFetchBytes(long maxSingleFetchBytes) {
            configBuilder.maxSingleFetchBytes(maxSingleFetchBytes);
            return this;
        }

        Builder model(DecisionTreePolicySelector model) {
            this.model = model;
            return this;
        }

        public AdaptiveReaderRuntime build() {
            if (fixedPolicy != null && prefixPolicies != null) {
                throw new IllegalStateException("forcePolicy and prefixPolicyMap cannot both be configured");
            }
            return new AdaptiveReaderRuntime(this);
        }
    }
}
