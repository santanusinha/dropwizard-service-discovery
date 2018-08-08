package io.dropwizard.discovery.common;

import lombok.*;

import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@EqualsAndHashCode
@ToString
public class DistributionShardInfo extends ProximityShardInfo {
    private String distributionId;
    List<String> list = new ArrayList<String>();

    public List<String> getIDs() {
        list.add(distributionId);
        return list;
    }
}
