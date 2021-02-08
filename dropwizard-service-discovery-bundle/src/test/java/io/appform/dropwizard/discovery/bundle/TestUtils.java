/*
 * Copyright (c) 2021 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.appform.dropwizard.discovery.bundle;

import io.dropwizard.Configuration;
import lombok.experimental.UtilityClass;
import org.awaitility.Awaitility;

import java.time.Duration;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 *
 */
@UtilityClass
public class TestUtils {
    public static void assertNodePresence(ServiceDiscoveryBundle<Configuration> bundle) {
        Awaitility.await()
                .pollInterval(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertNotNull(bundle.getServiceDiscoveryClient()
                                                           .getNode()
                                                           .orElse(null)));
    }
    public static void assertNodeAbsence(ServiceDiscoveryBundle<Configuration> bundle) {
        Awaitility.await()
                .pollInterval(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertNull(bundle.getServiceDiscoveryClient()
                                                           .getNode()
                                                           .orElse(null)));
    }
}
