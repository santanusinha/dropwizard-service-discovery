package io.appform.dropwizard.discovery.client.selector;

import com.flipkart.ranger.finder.sharded.MapBasedServiceRegistry;
import com.flipkart.ranger.model.ServiceNode;
import com.flipkart.ranger.model.ShardSelector;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import io.appform.dropwizard.discovery.common.ShardInfo;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

public class HierarchicalEnvironmentAwareShardSelector implements ShardSelector<ShardInfo, MapBasedServiceRegistry<ShardInfo>> {

    private static final String separator = ".";

    @Override
    public List<ServiceNode<ShardInfo>> nodes(final ShardInfo criteria,
                                              final MapBasedServiceRegistry<ShardInfo> serviceRegistry) {
        val serviceNodes = serviceRegistry.nodes();
        String environment = criteria.getEnvironment();
        while (!Strings.isNullOrEmpty(environment)) {
            val currentEnvNodes = serviceNodes.get(ShardInfo.builder()
                    .environment(environment)
                    .build());
            if (!currentEnvNodes.isEmpty()) {
                return currentEnvNodes;
            }
            environment = StringUtils.substringBeforeLast(environment, separator);
        }
        return Lists.newArrayList();
    }
}
