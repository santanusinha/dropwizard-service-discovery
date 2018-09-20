package io.dropwizard.discovery.common;

import com.flipkart.ranger.finder.RandomServiceNodeSelector;
import com.flipkart.ranger.model.ServiceNode;
import com.flipkart.ranger.model.ServiceNodeSelector;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

public class LocationAwareServiceNodeSelector implements ServiceNodeSelector<ShardInfo> {

    private ShardInfo shardInfo;
    private ServiceNodeSelector<ShardInfo> fallbackSelector;

    public LocationAwareServiceNodeSelector(ShardInfo shardInfo) {
        this.shardInfo = shardInfo;
        this.fallbackSelector = new RandomServiceNodeSelector<>();
    }

    @Override
    public ServiceNode<ShardInfo> select(List<ServiceNode<ShardInfo>> nodeList) {
        if (shardInfo.getLocationAttributes() == null
                || shardInfo.getLocationAttributes().isEmpty()) {
            return fallbackSelector.select(nodeList);
        }

        List<ServiceNode<ShardInfo>> candidateNodes = Lists.newArrayList(nodeList);
        for (int i = 0; i < shardInfo.getLocationAttributes().size(); i++) {
            List<ServiceNode<ShardInfo>> workingSetNodes = Lists.newArrayList();
            for (ServiceNode<ShardInfo> serviceNode : candidateNodes) {
                List<String> locationAttributes = serviceNode.getNodeData().getLocationAttributes();
                if (!CommonUtils.isNullOrEmpty(locationAttributes)
                        && locationAttributes.size() > i
                        && Objects.equals(locationAttributes.get(i), shardInfo.getLocationAttributes().get(i))) {
                    workingSetNodes.add(serviceNode);
                }
            }
            if (CommonUtils.isNullOrEmpty(workingSetNodes)) {
                break;
            }
            candidateNodes = workingSetNodes;
        }

        return fallbackSelector.select(candidateNodes);
    }
}
