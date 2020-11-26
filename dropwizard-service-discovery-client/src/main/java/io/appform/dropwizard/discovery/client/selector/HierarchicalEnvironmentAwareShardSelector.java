package io.appform.dropwizard.discovery.client.selector;

import com.flipkart.ranger.finder.sharded.MapBasedServiceRegistry;
import com.flipkart.ranger.model.ServiceNode;
import com.flipkart.ranger.model.ShardSelector;
import com.google.common.base.Strings;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import io.appform.dropwizard.discovery.client.Constants;
import io.appform.dropwizard.discovery.common.ShardInfo;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class HierarchicalEnvironmentAwareShardSelector implements ShardSelector<ShardInfo, MapBasedServiceRegistry<ShardInfo>> {

    private static final String SEPARATOR = ".";

    @Override
    public List<ServiceNode<ShardInfo>> nodes(final ShardInfo criteria,
                                              final MapBasedServiceRegistry<ShardInfo> serviceRegistry) {
        val serviceNodes = serviceRegistry.nodes();

        String environment = criteria.getEnvironment();

        if (Objects.equals(environment, Constants.ALL_ENV)) {
            return allNodes(serviceNodes);
        }

        while (!Strings.isNullOrEmpty(environment)) {
            val currentEnvNodes = serviceNodes.get(ShardInfo.builder()
                    .environment(environment)
                    .build());
            if (!currentEnvNodes.isEmpty()) {
                return currentEnvNodes;
            }

            if (!environment.contains(SEPARATOR)){
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
