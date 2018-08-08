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

package io.dropwizard.discovery.client.io.dropwizard.ranger;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ranger.ServiceFinderBuilders;
import com.flipkart.ranger.finder.sharded.SimpleShardedServiceFinder;
import com.flipkart.ranger.model.ServiceNode;
import io.dropwizard.discovery.common.DistributionShardInfo;
import io.dropwizard.discovery.common.ProximityShardInfo;
import io.dropwizard.discovery.common.RackShardInfo;
import io.dropwizard.discovery.common.ShardInfo;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;

import java.util.List;
import java.util.Optional;

/**
 * Client that returns a healthy node from nodes for a particular environment
 */
@Slf4j
public class ServiceDiscoveryClient {
    private final ShardInfo criteria;
    private SimpleShardedServiceFinder<ShardInfo> serviceFinder;
    private ProximityShardSelector proximityShardSelector = new ProximityShardSelector();

    @Builder(builderMethodName = "fromConnectionString", builderClassName = "FromConnectionStringBuilder")
    ServiceDiscoveryClient(String namespace, String serviceName, String environment,
                           ObjectMapper objectMapper, String connectionString) throws Exception {
        this(namespace, serviceName, environment, objectMapper,
                CuratorFrameworkFactory.newClient(connectionString, new RetryForever(5000)));
    }

    @Builder(builderMethodName = "fromConnectionStringWithDistributionInfo", builderClassName = "FromConnectionStringWithDistributionInfoBuilder")
    ServiceDiscoveryClient(String namespace, String serviceName, String environment, ObjectMapper objectMapper,
                           String connectionString, String distributionId, Double distributionProbability) throws Exception {
        this(namespace, serviceName, environment, objectMapper,
                CuratorFrameworkFactory.newClient(connectionString, new RetryForever(5000)), distributionId, distributionProbability);
    }

    @Builder(builderMethodName = "fromConnectionStringWithRackInfo", builderClassName = "FromConnectionStringWithRackInfoBuilder")
    ServiceDiscoveryClient(String namespace, String serviceName, String environment, ObjectMapper objectMapper,
                           String connectionString, String dcId, String rackId, String host,
                           Double dcProbability, Double rackProbability, Double hostProbability) throws Exception {
        this(namespace, serviceName, environment, objectMapper,
                CuratorFrameworkFactory.newClient(connectionString, new RetryForever(5000)),
                dcId, rackId, host, dcProbability, rackProbability, hostProbability);
    }

    @Builder(builderMethodName = "fromCurator", builderClassName = "FromCuratorBuilder")
    ServiceDiscoveryClient(String namespace, String serviceName, String environment,
                           ObjectMapper objectMapper, CuratorFramework curator) throws Exception {
        this.criteria = ShardInfo.builder().environment(environment).build();
        this.serviceFinder = ServiceFinderBuilders.<ShardInfo>shardedFinderBuilder()
                .withCuratorFramework(curator)
                .withNamespace(namespace)
                .withServiceName(serviceName)
                .withDeserializer(data -> {
                    try {
                        return objectMapper.readValue(data,
                                new TypeReference<ServiceNode<ShardInfo>>() {
                                });
                    } catch (Exception e) {
                        log.warn("Could not parse node data", e);
                    }
                    return null;
                })
                .build();
    }

    @Builder(builderMethodName = "fromCuratorWithDistributionInfo", builderClassName = "FromCuratorWithDistributionInfoBuilder")
    ServiceDiscoveryClient(String namespace, String serviceName, String environment,
                           ObjectMapper objectMapper, CuratorFramework curator, String distributionId, Double distributionProbability) throws Exception {
        ProximityShardInfo proximityShardInfo = DistributionShardInfo.builder().distributionId(distributionId).build();
        this.criteria = ShardInfo.builder().environment(environment).proximityShardInfo(proximityShardInfo).build();
        this.proximityShardSelector.setDistributionProbability(distributionProbability);
        this.proximityShardSelector.setSelectorType(ProximityShardSelector.SelectorType.DISTRIBUTION);
        this.serviceFinder = ServiceFinderBuilders.<ShardInfo>shardedFinderBuilder()
                .withCuratorFramework(curator)
                .withNamespace(namespace)
                .withServiceName(serviceName)
                .withShardSelector(proximityShardSelector)
                .withDeserializer(data -> {
                    try {
                        return objectMapper.readValue(data,
                                new TypeReference<ServiceNode<ShardInfo>>() {
                                });
                    } catch (Exception e) {
                        log.warn("Could not parse node data", e);
                    }
                    return null;
                })
                .build();
    }

    @Builder(builderMethodName = "fromCuratorWithRackInfo", builderClassName = "FromCuratorWithRackInfoBuilder")
    ServiceDiscoveryClient(String namespace, String serviceName, String environment,
                           ObjectMapper objectMapper, CuratorFramework curator, String dcId, String rackId, String host,
                           Double dcProbability, Double rackProbability, Double hostProbability) throws Exception {
        ProximityShardInfo proximityShardInfo = RackShardInfo.builder().dcId(dcId).rackId(rackId).host(host).build();
        this.criteria = ShardInfo.builder().environment(environment).proximityShardInfo(proximityShardInfo).build();
        this.proximityShardSelector.setDcProbability(dcProbability);
        this.proximityShardSelector.setRackProbability(rackProbability);
        this.proximityShardSelector.setHostProbability(hostProbability);
        this.proximityShardSelector.setSelectorType(ProximityShardSelector.SelectorType.RACK);
        this.serviceFinder = ServiceFinderBuilders.<ShardInfo>shardedFinderBuilder()
                .withCuratorFramework(curator)
                .withNamespace(namespace)
                .withServiceName(serviceName)
                .withShardSelector(proximityShardSelector)
                .withDeserializer(data -> {
                    try {
                        return objectMapper.readValue(data,
                                new TypeReference<ServiceNode<ShardInfo>>() {
                                });
                    } catch (Exception e) {
                        log.warn("Could not parse node data", e);
                    }
                    return null;
                })
                .build();
    }

    public void start() throws Exception {
        serviceFinder.start();
    }

    public void stop() throws Exception {
        serviceFinder.stop();
    }

    public Optional<ServiceNode<ShardInfo>> getNode() {
        return Optional.ofNullable(serviceFinder.get(criteria));
    }

    public List<ServiceNode<ShardInfo>> getAllNodes() {
        return serviceFinder.getAll(criteria);
    }
}
