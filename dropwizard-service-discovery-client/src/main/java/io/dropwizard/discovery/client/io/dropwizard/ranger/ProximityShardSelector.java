package io.dropwizard.discovery.client.io.dropwizard.ranger;

import com.flipkart.ranger.finder.sharded.MapBasedServiceRegistry;
import com.flipkart.ranger.model.ServiceNode;
import com.flipkart.ranger.model.ShardSelector;
import io.dropwizard.discovery.common.ShardInfo;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

public class ProximityShardSelector<T> implements ShardSelector<ShardInfo, MapBasedServiceRegistry<ShardInfo>> {
    private String distributionId;
    private double probability;

    public String getDistributionId() {
        return distributionId;
    }

    public void setDistributionId(String distributionId) {
        this.distributionId = distributionId;
    }

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }

    @Override
    public List<ServiceNode<ShardInfo>> nodes(ShardInfo criteria, MapBasedServiceRegistry<ShardInfo> serviceRegistry) {
        List<ServiceNode<ShardInfo>> nodesList =  serviceRegistry.nodes().get(criteria);
        List<ServiceNode<ShardInfo>> serviceNodes = new ArrayList<ServiceNode<ShardInfo>>();
        SecureRandom secureRandom = new SecureRandom();

        int randomNo = secureRandom.nextInt(100);
        boolean providedDistributionIdSelected = true;

        //as per probability here we are setting weather we should go ahead with distribution id rack or not
        if(randomNo < probability * 100) {
            providedDistributionIdSelected = true;
        } else {
            providedDistributionIdSelected = false;
        }

        //getting the nodes corresponding to distributionId
        for(ServiceNode<ShardInfo> node : nodesList) {
            if(providedDistributionIdSelected && distributionId == node.getNodeData().getDistributionId()){
                serviceNodes.add(node);
            } else if(!providedDistributionIdSelected && distributionId != node.getNodeData().getDistributionId()) {
                serviceNodes.add(node);
            }
        }

        return serviceNodes;
    }
}
