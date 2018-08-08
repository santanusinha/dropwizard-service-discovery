/*
 * Copyright (c) 2016 Santanu Sinha <santanu.sinha@gmail.com>
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

package io.dropwizard.discovery.bundle;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ranger.healthcheck.HealthcheckStatus;
import com.flipkart.ranger.model.ServiceNode;
import io.dropwizard.Configuration;
import io.dropwizard.discovery.common.ShardInfo;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import io.dropwizard.setup.AdminEnvironment;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingCluster;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


@Slf4j
public class ServiceDiscoveryBundleTest {

    private final HealthCheckRegistry healthChecks = mock(HealthCheckRegistry.class);
    private final JerseyEnvironment jerseyEnvironment = mock(JerseyEnvironment.class);
    private final LifecycleEnvironment lifecycleEnvironment = new LifecycleEnvironment();
    private final Environment environment = mock(Environment.class);
    private final Bootstrap<?> bootstrap = mock(Bootstrap.class);
    private final Configuration configuration = mock(Configuration.class);


    private final ServiceDiscoveryBundle<Configuration> bundle = new ServiceDiscoveryBundle<Configuration>() {
        @Override
        protected ServiceDiscoveryConfiguration getRangerConfiguration(Configuration configuration) {
            return serviceDiscoveryConfiguration;
        }

        @Override
        protected String getServiceName(Configuration configuration) {
            return "TestService";
        }

    };

    private ServiceDiscoveryConfiguration serviceDiscoveryConfiguration;
    private final TestingCluster testingCluster = new TestingCluster(1);

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private HealthcheckStatus status = HealthcheckStatus.healthy;

    @Before
    public void setup() throws Exception {

        when(jerseyEnvironment.getResourceConfig()).thenReturn(new DropwizardResourceConfig());
        when(environment.jersey()).thenReturn(jerseyEnvironment);
        when(environment.lifecycle()).thenReturn(lifecycleEnvironment);
        when(environment.healthChecks()).thenReturn(healthChecks);
        when(environment.getObjectMapper()).thenReturn(new ObjectMapper());
        AdminEnvironment adminEnvironment = mock(AdminEnvironment.class);
        doNothing().when(adminEnvironment).addTask(any());
        when(environment.admin()).thenReturn(adminEnvironment);

        testingCluster.start();

        serviceDiscoveryConfiguration = ServiceDiscoveryConfiguration.builder()
                                    .zookeeper(testingCluster.getConnectString())
                                    .namespace("test")
                                    .environment("testing")
                                    .connectionRetryIntervalMillis(5000)
                                    .publishedHost("TestHost")
                                    .publishedPort(8021)
                                    .initialRotationStatus(true)
                                    .build();
        bundle.initialize(bootstrap);

        bundle.run(configuration, environment);
        final AtomicBoolean started = new AtomicBoolean(false);
        executorService.submit(() -> lifecycleEnvironment.getManagedObjects().forEach(object -> {
            try {
                object.start();
                started.set(true);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        while (!started.get()) {
            Thread.sleep(1000);
            log.debug("Waiting for framework to start...");
        }

        bundle.registerHealthcheck(() -> status);
    }

    @Test
    public void testDiscovery() throws Exception {
        Optional<ServiceNode<ShardInfo>> info = bundle.getServiceDiscoveryClient().getNode();
        System.out.println(environment.getObjectMapper().writeValueAsString(info));
        assertTrue(info.isPresent());
        assertEquals("testing", info.get().getNodeData().getEnvironment());
        assertEquals("TestHost", info.get().getHost());
        assertEquals(8021, info.get().getPort());
        status = HealthcheckStatus.unhealthy;

        Thread.sleep(10000);
        info = bundle.getServiceDiscoveryClient().getNode();
        assertFalse(info.isPresent());
    }
}