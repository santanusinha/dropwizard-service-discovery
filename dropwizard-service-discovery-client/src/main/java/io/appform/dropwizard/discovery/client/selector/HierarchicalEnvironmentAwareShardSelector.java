package io.appform.dropwizard.discovery.client.selector;

import com.flipkart.ranger.finder.sharded.MapBasedServiceRegistry;
import com.flipkart.ranger.model.ServiceNode;
import com.flipkart.ranger.model.ShardSelector;
import com.google.common.base.Strings;
import com.google.common.collect.ListMultimap;
import io.appform.dropwizard.discovery.client.Constants;
import io.appform.dropwizard.discovery.common.ShardInfo;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Slf4j
public class HierarchicalEnvironmentAwareShardSelector implements ShardSelector<ShardInfo, MapBasedServiceRegistry<ShardInfo>> {

    private static final String SEPARATOR = ".";

    @Override
    public List<ServiceNode<ShardInfo>> nodes(final ShardInfo criteria,
                                              final MapBasedServiceRegistry<ShardInfo> serviceRegistry) {
        val serviceNodes = serviceRegistry.nodes();
        val serviceName = serviceRegistry.getService().getServiceName();

        String environment = criteria.getEnvironment();

        if (Objects.equals(environment, Constants.ALL_ENV)) {
            return allNodes(serviceNodes);
        }

        while (!Strings.isNullOrEmpty(environment)) {
            val currentEnvNodes = serviceNodes.get(ShardInfo.builder()
                    .environment(environment)
                    .build());
            if (!currentEnvNodes.isEmpty()) {
                log.debug("Effective environment for discovery of {} is {}", serviceName, environment);
                return currentEnvNodes;
            }

            if (!environment.contains(SEPARATOR)) {
                log.debug("Effective environment for discovery of {} is {}", serviceName, environment);
                return Collections.emptyList();
            }

            environment = StringUtils.substringBeforeLast(environment, SEPARATOR);
        }
        return Collections.emptyList();
    }

    private List<ServiceNode<ShardInfo>> allNodes(ListMultimap<ShardInfo, ServiceNode<ShardInfo>> serviceNodes) {
        return serviceNodes.asMap()
                .values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
