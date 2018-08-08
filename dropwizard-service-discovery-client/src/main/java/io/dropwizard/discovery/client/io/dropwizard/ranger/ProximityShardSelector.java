package io.dropwizard.discovery.client.io.dropwizard.ranger;

import com.flipkart.ranger.finder.sharded.MapBasedServiceRegistry;
import com.flipkart.ranger.model.ServiceNode;
import com.flipkart.ranger.model.ShardSelector;
import com.google.common.collect.ListMultimap;
import io.dropwizard.discovery.common.ShardInfo;
import lombok.Getter;
import lombok.Setter;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

public class ProximityShardSelector implements ShardSelector<ShardInfo, MapBasedServiceRegistry<ShardInfo>> {
    @Getter
    @Setter
    private Double distributionProbability;

    @Getter
    @Setter
    private Double dcProbability;

    @Getter
    @Setter
    private Double rackProbability;

    @Getter
    @Setter
    private Double hostProbability;

    @Getter
    @Setter
    private SelectorType selectorType;

    SecureRandom secureRandom = new SecureRandom();
    //List of Service Nodes with the required environment
    List<ServiceNode<ShardInfo>> environmentNodesList = new ArrayList<ServiceNode<ShardInfo>>();
    //List of service nodes with the required environment and distributionId
    List<ServiceNode<ShardInfo>> distributionNodesList = new ArrayList<ServiceNode<ShardInfo>>();
    //List of service nodes with the required environment and rackId
    List<ServiceNode<ShardInfo>> rackNodesList = new ArrayList<ServiceNode<ShardInfo>>();
    List<ServiceNode<ShardInfo>> dcNodesList = new ArrayList<ServiceNode<ShardInfo>>();
    List<ServiceNode<ShardInfo>> hostNodesList = new ArrayList<ServiceNode<ShardInfo>>();
    //final nodes List
    List<ServiceNode<ShardInfo>> nodesList = new ArrayList<ServiceNode<ShardInfo>>();
    List<ServiceNode<ShardInfo>> tempNodesList = new ArrayList<ServiceNode<ShardInfo>>();


    @Override
    public List<ServiceNode<ShardInfo>> nodes(ShardInfo shardInfo, MapBasedServiceRegistry<ShardInfo> serviceRegistry) {
        ListMultimap<ShardInfo, ServiceNode<ShardInfo>> serviceNodes =  serviceRegistry.nodes();
        for(ShardInfo key : serviceNodes.keySet()) {
            if(key.getEnvironment() == shardInfo.getEnvironment()){
                environmentNodesList.addAll(serviceNodes.get(key));
            }
        }

        if(selectorType == SelectorType.DISTRIBUTION) {
            nodesList = distributionShardSelectorNodes(shardInfo);
        } else {
            nodesList = rackShardSelectorNodes(shardInfo);
        }

        return nodesList;
    }

    private List<ServiceNode<ShardInfo>> distributionShardSelectorNodes(ShardInfo shardInfo) {
        String distributionId = shardInfo.getProximityShardInfo().getIDs().get(0);

        if(distributionId == null || distributionProbability == null) {
            return environmentNodesList;
        }

        boolean providedDistributionIdSelected = isProvidedIdSelected(distributionProbability);

        //getting the nodes corresponding to distributionId
        for(ServiceNode<ShardInfo> node : environmentNodesList) {
            if(providedDistributionIdSelected && distributionId == node.getNodeData().getProximityShardInfo().getIDs().get(0)){
                distributionNodesList.add(node);
            } else if(!providedDistributionIdSelected && distributionId != node.getNodeData().getProximityShardInfo().getIDs().get(0)) {
                distributionNodesList.add(node);
            }
        }

        //just in case if as per probability we get no nodes in distributionNodesList
        if(distributionNodesList == null) {
            return environmentNodesList;
        }

        return distributionNodesList;
    }

    private List<ServiceNode<ShardInfo>> rackShardSelectorNodes(ShardInfo shardInfo){
        String dcId = shardInfo.getProximityShardInfo().getIDs().get(0);
        String rackId = shardInfo.getProximityShardInfo().getIDs().get(1);
        String host = shardInfo.getProximityShardInfo().getIDs().get(2);

        dcNodesList = filteredNodesList(dcId, dcProbability, environmentNodesList, IDtype.DC);
        //in case if as per probability we get no nodes in dcNodesList
        if(dcNodesList == null) {
            return environmentNodesList;
        }
        rackNodesList = filteredNodesList(rackId, rackProbability, dcNodesList, IDtype.RACK);
        //in case if as per probability we get no nodes in rackNodesList
        if(rackNodesList == null) {
            return dcNodesList;
        }
        hostNodesList = filteredNodesList(host, hostProbability, rackNodesList, IDtype.HOST);
        //in case if as per probability we get no nodes in hostNodesList
        if(hostNodesList == null) {
            return rackNodesList;
        }

        return hostNodesList;
    }

    private List<ServiceNode<ShardInfo>> filteredNodesList(String id, Double probability, List<ServiceNode<ShardInfo>> inputNodesList, IDtype idtype ) {
        tempNodesList.clear();
        if(id == null || probability == null) {
            return inputNodesList;
        }

        boolean providedIdSelected = isProvidedIdSelected(probability);

        //getting the nodes corresponding to Id
        for(ServiceNode<ShardInfo> node : inputNodesList) {
            if(providedIdSelected && id == idtype.getId(node)){
                tempNodesList.add(node);
            } else if(!providedIdSelected && id != idtype.getId(node)) {
                tempNodesList.add(node);
            }
        }

        return tempNodesList;
    }

    private boolean isProvidedIdSelected(Double probability) {
        if(secureRandom.nextInt(100) < probability * 100) {
            return true;
        } else {
            return false;
        }
    }

    private enum IDtype {

        DC {
            @Override
            public String getId(ServiceNode<ShardInfo> node) {
                return node.getNodeData().getProximityShardInfo().getIDs().get(0);
            }
        },
        RACK {
            @Override
            public String getId(ServiceNode<ShardInfo> node) {
                return node.getNodeData().getProximityShardInfo().getIDs().get(1);
            }
        },
        HOST {
            @Override
            public String getId(ServiceNode<ShardInfo> node) {
                return node.getNodeData().getProximityShardInfo().getIDs().get(2);
            }
        };

        public abstract String getId(ServiceNode<ShardInfo> node);
    }

    public enum SelectorType {
        DISTRIBUTION, RACK;
    }
}
