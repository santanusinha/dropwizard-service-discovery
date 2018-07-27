package io.dropwizard.discovery.common;

import com.flipkart.ranger.finder.RandomServiceNodeSelector;
import com.flipkart.ranger.model.ServiceNode;
import com.flipkart.ranger.model.ServiceNodeSelector;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class LocationAwareServiceNodeSelector implements ServiceNodeSelector<ShardInfo> {

    private ShardInfo shardInfo;
    private ServiceNodeSelector<ShardInfo> fallbackSelector;

    public LocationAwareServiceNodeSelector(ShardInfo shardInfo) {
        this.shardInfo = shardInfo;
        this.fallbackSelector = new RandomServiceNodeSelector<>();
    }

    @Override
    public ServiceNode<ShardInfo> select(List<ServiceNode<ShardInfo>> nodeList) {
        List<ServiceNode<ShardInfo>> candidateNodes = Lists.newArrayList(nodeList);
        if (!Strings.isNullOrEmpty(shardInfo.getDcId())) {
            candidateNodes = candidateNodes.stream()
                    .filter(x -> Objects.equals(x.getNodeData().getDcId(), shardInfo.getDcId()))
                    .collect(Collectors.toList());
        }

        if (candidateNodes.isEmpty()) {
            return fallbackSelector.select(nodeList);
        }

        if (!Strings.isNullOrEmpty(shardInfo.getRackId())) {
            candidateNodes = candidateNodes.stream()
                    .filter(x -> Objects.equals(x.getNodeData().getDcId(), shardInfo.getDcId()))
                    .collect(Collectors.toList());
        }

        if (candidateNodes.isEmpty()) {
            return fallbackSelector.select(nodeList);
        }

        return fallbackSelector.select(candidateNodes);
    }
}
