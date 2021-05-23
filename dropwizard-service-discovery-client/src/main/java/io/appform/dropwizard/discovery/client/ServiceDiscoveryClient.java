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

package io.appform.dropwizard.discovery.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.ranger.ServiceFinderBuilders;
import com.flipkart.ranger.finder.sharded.SimpleShardedServiceFinder;
import com.flipkart.ranger.model.ServiceNode;
import io.appform.dropwizard.discovery.client.selector.HierarchicalEnvironmentAwareShardSelector;
import io.appform.dropwizard.discovery.common.ShardInfo;
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

    @Builder(builderMethodName = "fromConnectionString", builderClassName = "FromConnectionStringBuilder")
    private ServiceDiscoveryClient(
            String namespace,
            String serviceName,
            String environment,
            ObjectMapper objectMapper,
            String connectionString,
            int refreshTimeMs,
            boolean disableWatchers) {
        this(namespace,
                serviceName,
                environment,
                objectMapper,
                CuratorFrameworkFactory.newClient(connectionString, new RetryForever(5000)),
                refreshTimeMs,
                disableWatchers);
    }

    @Builder(builderMethodName = "fromCurator", builderClassName = "FromCuratorBuilder")
    ServiceDiscoveryClient(
            String namespace,
            String serviceName,
            String environment,
            ObjectMapper objectMapper,
            CuratorFramework curator,
            int refreshTimeMs,
            boolean disableWatchers) {

        int effectiveRefreshTimeMs = refreshTimeMs;
        if (effectiveRefreshTimeMs < Constants.MINIMUM_REFRESH_TIME) {
            effectiveRefreshTimeMs = Constants.MINIMUM_REFRESH_TIME;
            log.warn("Node info update interval too low: {} ms. Has been upgraded to {} ms ",
                    refreshTimeMs,
                    Constants.MINIMUM_REFRESH_TIME);
        }

        this.criteria = ShardInfo.builder()
                .environment(environment)
                .build();
        this.serviceFinder = ServiceFinderBuilders.<ShardInfo>shardedFinderBuilder()
                .withCuratorFramework(curator)
                .withNamespace(namespace)
                .withServiceName(serviceName)
                .withDeserializer(data -> {
                    try {
                        return objectMapper.readValue(data,
                                new TypeReference<ServiceNode<ShardInfo>>() {
                                });
                    }
                    catch (Exception e) {
                        log.warn("Could not parse node data", e);
                    }
                    return null;
                })
                .withNodeRefreshIntervalMs(effectiveRefreshTimeMs)
                .withDisableWatchers(disableWatchers)
                .withShardSelector(new HierarchicalEnvironmentAwareShardSelector())
                .build();
    }

    public void start() throws Exception {
        serviceFinder.start();
    }

    public void stop() throws Exception {
        serviceFinder.stop();
    }

    public Optional<ServiceNode<ShardInfo>> getNode() {
        return getNode(criteria);
    }

    public List<ServiceNode<ShardInfo>> getAllNodes() {
        return getAllNodes(criteria);
    }

    public Optional<ServiceNode<ShardInfo>> getNode(final ShardInfo shardInfo) {
            return Optional.ofNullable(serviceFinder.get(shardInfo));
    }

    public List<ServiceNode<ShardInfo>> getAllNodes(final ShardInfo shardInfo) {
        return serviceFinder.getAll(shardInfo);
    }


}
