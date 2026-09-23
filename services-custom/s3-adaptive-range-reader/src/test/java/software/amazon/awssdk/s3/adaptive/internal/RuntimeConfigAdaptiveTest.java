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

package software.amazon.awssdk.s3.adaptive.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class RuntimeConfigAdaptiveTest {

    @AfterEach
    void clear() {
        System.clearProperty("track1.d1.adaptive");
        System.clearProperty("track1.d1.fixed.capacity");
        System.clearProperty("track1.d1.fixed.admission");
        System.clearProperty("track1.d1.cache.mib");
        System.clearProperty("track1.d1.hard.mib");
        System.clearProperty("track1.d1.coverage");
        System.clearProperty("track1.d1.min.mib");
        System.clearProperty("track1.d1.observe.gets");
        System.clearProperty("track1.d1.horizon.mib");
        System.clearProperty("track1.d1.horizon.events");
    }

    @Test
    void adaptiveRaisesHardBudgetAndKeepsObserveFromCacheMib() {
        System.setProperty("track1.d1.adaptive", "true");
        System.setProperty("track1.d1.cache.mib", "256");
        System.setProperty("track1.d1.hard.mib", "4096");
        System.setProperty("track1.d1.coverage", "0.5");
        RuntimeConfig cfg = RuntimeConfig.fromSystemProperties();
        assertThat(cfg.d1Adaptive()).isTrue();
        assertThat(cfg.d1ObserveBudgetBytes()).isEqualTo(256L * 1024 * 1024);
        assertThat(cfg.d1HardCacheBytes()).isEqualTo(4096L * 1024 * 1024);
        assertThat(cfg.globalCacheBytes()).isEqualTo(4096L * 1024 * 1024);
        assertThat(cfg.d1TargetCoverage()).isEqualTo(0.5);
    }

    @Test
    void adaptiveDefaultsCoverFullAdmittedWorkingSetAndByteHorizon() {
        System.setProperty("track1.d1.adaptive", "true");
        System.setProperty("track1.d1.cache.mib", "256");
        RuntimeConfig cfg = RuntimeConfig.fromSystemProperties();
        assertThat(cfg.d1TargetCoverage()).isEqualTo(1.0);
        assertThat(cfg.d1HorizonBytes()).isEqualTo(4096L * 1024 * 1024);
        assertThat(cfg.d1HorizonEvents()).isEqualTo(131072);
    }

    @Test
    void frozenUsesCacheMibAsHardBudget() {
        System.setProperty("track1.d1.adaptive", "false");
        System.setProperty("track1.d1.cache.mib", "2048");
        RuntimeConfig cfg = RuntimeConfig.fromSystemProperties();
        assertThat(cfg.d1Adaptive()).isFalse();
        assertThat(cfg.d1HardCacheBytes()).isEqualTo(2048L * 1024 * 1024);
        assertThat(cfg.globalCacheBytes()).isEqualTo(2048L * 1024 * 1024);
    }

    @Test
    void adaptiveCanPinCapacityAndAdmissionIndependently() {
        System.setProperty("track1.d1.adaptive", "true");
        System.setProperty("track1.d1.fixed.capacity", "true");
        System.setProperty("track1.d1.fixed.admission", "true");
        RuntimeConfig cfg = RuntimeConfig.fromSystemProperties();
        assertThat(cfg.d1FixedCapacity()).isTrue();
        assertThat(cfg.d1FixedAdmission()).isTrue();
    }
}
