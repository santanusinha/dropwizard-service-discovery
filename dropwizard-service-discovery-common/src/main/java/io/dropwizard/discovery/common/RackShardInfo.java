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
public class RackShardInfo extends ProximityShardInfo {
    private String dcId;
    private String rackId;
    private String host;
    List<String> list = new ArrayList<String>();

    public List<String> getIDs() {
        list.add(dcId);
        list.add(rackId);
        list.add(host);
        return list;
    }
}
