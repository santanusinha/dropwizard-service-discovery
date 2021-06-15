/*
 * Copyright (c) 2019 Santanu Sinha <santanu.sinha@gmail.com>
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ranger.ServiceProviderBuilders;
import com.flipkart.ranger.healthcheck.Healthcheck;
import com.flipkart.ranger.healthservice.TimeEntity;
import com.flipkart.ranger.healthservice.monitor.IsolatedHealthMonitor;
import com.flipkart.ranger.serviceprovider.ServiceProvider;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.appform.dropwizard.discovery.bundle.healthchecks.InitialDelayChecker;
import io.appform.dropwizard.discovery.bundle.healthchecks.InternalHealthChecker;
import io.appform.dropwizard.discovery.bundle.healthchecks.RotationCheck;
import io.appform.dropwizard.discovery.bundle.id.IdGenerator;
import io.appform.dropwizard.discovery.bundle.id.NodeIdManager;
import io.appform.dropwizard.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.dropwizard.discovery.bundle.monitors.DropwizardHealthMonitor;
import io.appform.dropwizard.discovery.bundle.monitors.DropwizardServerStartupCheck;
import io.appform.dropwizard.discovery.bundle.rotationstatus.BIRTask;
import io.appform.dropwizard.discovery.bundle.rotationstatus.DropwizardServerStatus;
import io.appform.dropwizard.discovery.bundle.rotationstatus.OORTask;
import io.appform.dropwizard.discovery.bundle.rotationstatus.RotationStatus;
import io.appform.dropwizard.discovery.client.ServiceDiscoveryClient;
import io.appform.dropwizard.discovery.common.ShardInfo;
import io.dropwizard.Configuration;
import io.dropwizard.ConfiguredBundle;
import io.dropwizard.lifecycle.Managed;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * A dropwizard bundle for service discovery.
 */
@Slf4j
public abstract class ServiceDiscoveryBundle<T extends Configuration> implements ConfiguredBundle<T> {

    private ServiceDiscoveryConfiguration serviceDiscoveryConfiguration;
    private List<Healthcheck> healthchecks = Lists.newArrayList();
    private ServiceProvider<ShardInfo> serviceProvider;
    private final List<IdValidationConstraint> globalIdConstraints;

    @Getter
    private CuratorFramework curator;

    @Getter
    private ServiceDiscoveryClient serviceDiscoveryClient;

    @Getter
    @VisibleForTesting
    private RotationStatus rotationStatus;

    @Getter
    @VisibleForTesting
    private DropwizardServerStatus serverStatus;

    protected ServiceDiscoveryBundle() {
        globalIdConstraints = Collections.emptyList();
    }

    protected ServiceDiscoveryBundle(List<IdValidationConstraint> globalIdConstraints) {
        this.globalIdConstraints = globalIdConstraints != null
                ? globalIdConstraints
                : Collections.emptyList();
    }

    @Override
    public void initialize(Bootstrap<?> bootstrap) {

    }

    @Override
    public void run(T configuration, Environment environment) throws Exception {
        serviceDiscoveryConfiguration = getRangerConfiguration(configuration);
        val objectMapper = environment.getObjectMapper();
        final String namespace = serviceDiscoveryConfiguration.getNamespace();
        final String serviceName = getServiceName(configuration);
        final String hostname = getHost();
        final int port = getPort(configuration);
        final int zoneId = getZoneId(configuration);

        rotationStatus = new RotationStatus(serviceDiscoveryConfiguration.isInitialRotationStatus());
        serverStatus = new DropwizardServerStatus(false);

        curator = CuratorFrameworkFactory.builder()
                .connectString(serviceDiscoveryConfiguration.getZookeeper())
                .namespace(namespace)
                .retryPolicy(new RetryForever(serviceDiscoveryConfiguration.getConnectionRetryIntervalMillis()))
                .build();
        serviceProvider = buildServiceProvider(
                environment,
                objectMapper,
                namespace,
                serviceName,
                hostname,
                port
        );
        serviceDiscoveryClient = buildDiscoveryClient(
                environment,
                namespace,
                serviceName);

        environment.lifecycle()
                .manage(new ServiceDiscoveryManager(serviceName, zoneId));
        environment.jersey()
                .register(new InfoResource(serviceDiscoveryClient));
        environment.admin()
                .addTask(new OORTask(rotationStatus));
        environment.admin()
                .addTask(new BIRTask(rotationStatus));
    }

    protected abstract ServiceDiscoveryConfiguration getRangerConfiguration(T configuration);

    protected abstract String getServiceName(T configuration);

    protected int getZoneId(T configuration) {
        Preconditions.checkArgument(
                serviceDiscoveryConfiguration.getZoneId() >= Constants.DEFAULT_ZONE_ID
                        && serviceDiscoveryConfiguration.getZoneId() < Constants.MAX_ZONES,
                String.format("Looks like zoneId has not been set correctly.\n" +
                        "Expected zoneId >= %d and < %d", Constants.DEFAULT_ZONE_ID, Constants.MAX_ZONES)
        );
        return serviceDiscoveryConfiguration.getZoneId();
    }

    protected List<IsolatedHealthMonitor> getHealthMonitors() {
        return Lists.newArrayList();
    }

    protected int getPort(T configuration) {
        Preconditions.checkArgument(
                Constants.DEFAULT_PORT != serviceDiscoveryConfiguration.getPublishedPort()
                        && 0 != serviceDiscoveryConfiguration.getPublishedPort(),
                "Looks like publishedPost has not been set and getPort() has not been overridden. This is wrong. \n" +
                        "Either set publishedPort in config or override getPort() to return the port on which the service is running");
        return serviceDiscoveryConfiguration.getPublishedPort();
    }

    protected String getHost() throws UnknownHostException {
        final String host = serviceDiscoveryConfiguration.getPublishedHost();

        if (Strings.isNullOrEmpty(host) || host.equals(Constants.DEFAULT_HOST)) {
            return InetAddress.getLocalHost()
                    .getCanonicalHostName();
        }
        return host;
    }

    public void registerHealthcheck(Healthcheck healthcheck) {
        this.healthchecks.add(healthcheck);
    }

    public void registerHealthchecks(List<Healthcheck> healthchecks) {
        this.healthchecks.addAll(healthchecks);
    }


    private ServiceDiscoveryClient buildDiscoveryClient(
            Environment environment,
            String namespace,
            String serviceName) {
        return ServiceDiscoveryClient.fromCurator()
                .curator(curator)
                .namespace(namespace)
                .serviceName(serviceName)
                .environment(serviceDiscoveryConfiguration.getEnvironment())
                .objectMapper(environment.getObjectMapper())
                .refreshTimeMs(serviceDiscoveryConfiguration.getRefreshTimeMs())
                .disableWatchers(serviceDiscoveryConfiguration.isDisableWatchers())
                .build();
    }

    private ServiceProvider<ShardInfo> buildServiceProvider(
            Environment environment,
            ObjectMapper objectMapper,
            String namespace,
            String serviceName,
            String hostname,
            int port) {
        val nodeInfo = ShardInfo.builder()
                .environment(serviceDiscoveryConfiguration.getEnvironment())
                .build();
        val initialDelayForMonitor = serviceDiscoveryConfiguration.getInitialDelaySeconds() > 1
                ? serviceDiscoveryConfiguration.getInitialDelaySeconds() - 1
                : 0;
        val dwMonitoringInterval = serviceDiscoveryConfiguration.getDropwizardCheckInterval() == 0
                ? Constants.DEFAULT_DW_CHECK_INTERVAl
                : serviceDiscoveryConfiguration.getDropwizardCheckInterval();
        val dwMonitoringStaleness = serviceDiscoveryConfiguration.getDropwizardCheckStaleness() < dwMonitoringInterval + 1
                ? dwMonitoringInterval + 1
                : serviceDiscoveryConfiguration.getDropwizardCheckStaleness();
        val serviceProviderBuilder = ServiceProviderBuilders.<ShardInfo>shardedServiceProviderBuilder()
                .withCuratorFramework(curator)
                .withNamespace(namespace)
                .withServiceName(serviceName)
                .withSerializer(data -> {
                    try {
                        return objectMapper.writeValueAsBytes(data);
                    }
                    catch (Exception e) {
                        log.warn("Could not parse node data", e);
                    }
                    return null;
                })
                .withHostname(hostname)
                .withPort(port)
                .withNodeData(nodeInfo)
                .withHealthcheck(new InternalHealthChecker(healthchecks))
                .withHealthcheck(new RotationCheck(rotationStatus))
                .withHealthcheck(new InitialDelayChecker(serviceDiscoveryConfiguration.getInitialDelaySeconds()))
                .withHealthcheck(new DropwizardServerStartupCheck(environment, serverStatus))
                .withIsolatedHealthMonitor(
                        new DropwizardHealthMonitor(
                                new TimeEntity(initialDelayForMonitor, dwMonitoringInterval, TimeUnit.SECONDS),
                                dwMonitoringStaleness * 1_000, environment))
                .withHealthUpdateIntervalMs(serviceDiscoveryConfiguration.getRefreshTimeMs())
                .withStaleUpdateThresholdMs(10000);

        val healthMonitors = getHealthMonitors();
        if (healthMonitors != null && !healthMonitors.isEmpty()) {
            healthMonitors.forEach(serviceProviderBuilder::withIsolatedHealthMonitor);
        }
        return serviceProviderBuilder.buildServiceDiscovery();
    }

    private class ServiceDiscoveryManager implements Managed {
        private final String serviceName;
        private final int zoneId;

        public ServiceDiscoveryManager(final String serviceName, final int zoneId) {
            this.serviceName = serviceName;
            this.zoneId = zoneId;
        }

        @Override
        public void start() throws Exception {
            curator.start();
            serviceProvider.start();
            serviceDiscoveryClient.start();
            NodeIdManager nodeIdManager = new NodeIdManager(curator, serviceName, zoneId);
            IdGenerator.initialize(nodeIdManager.fixNodeId(), globalIdConstraints, Collections.emptyMap());
        }

        @Override
        public void stop() throws Exception {
            serviceDiscoveryClient.stop();
            serviceProvider.stop();
            curator.close();
            IdGenerator.cleanUp();
        }
    }
}
