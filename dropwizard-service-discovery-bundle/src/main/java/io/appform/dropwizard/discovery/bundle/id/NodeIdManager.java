/*
 * Copyright (c) 2016 Santanu Sinha <santanu.sinha@gmail.com>
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

package io.appform.dropwizard.discovery.bundle.id;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.google.common.base.Predicates;
import io.appform.dropwizard.discovery.bundle.Constants;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.security.SecureRandom;
import java.util.concurrent.ExecutionException;

/**
 * Created by santanu on 2/5/16.
 */
@Slf4j
public class NodeIdManager {

    private final CuratorFramework curatorFramework;
    private final SecureRandom secureRandom;
    private final CuratorPathUtils pathUtils;

    private final int zoneId;

    @Getter
    private int node;

    public NodeIdManager(final CuratorFramework curatorFramework,
                         final String processName,
                         final int zoneId) {
        this.zoneId = zoneId;
        this.curatorFramework = curatorFramework;
        this.secureRandom = new SecureRandom(Long.toBinaryString(System.currentTimeMillis()).getBytes());
        this.pathUtils = new CuratorPathUtils(processName);
    }

    public int fixNodeId() {
        try {
            log.info("Waiting for curator to start");
            curatorFramework.blockUntilConnected();
            log.info("Curator started");
        } catch (InterruptedException e) {
            log.error("Wait for curator start interrupted", e);
        }
        Retryer<Boolean> retryer = RetryerBuilder.<Boolean>newBuilder()
                .retryIfResult(Predicates.equalTo(false))
                .retryIfException()
                .withStopStrategy(StopStrategies.neverStop())
                .build();
        try {
            retryer.call(() -> {
                node = (zoneId * Constants.MAX_NODES_PER_ZONE) + secureRandom.nextInt(Constants.MAX_NODES_PER_ZONE);
                final String path = pathUtils.path(node);
                try {
                    curatorFramework.create()
                            .creatingParentContainersIfNeeded()
                            .withMode(CreateMode.EPHEMERAL)
                            .forPath(path);
                } catch (KeeperException.NodeExistsException e) {
                    log.warn("Collision on node {}, will retry with new node.", node);
                    return false;
                }
                log.info("Node will be set to node id {}", node);
                return true;
            });
        } catch (RetryException e) {
            log.error("Error creating node", e);
        } catch (ExecutionException e) {
            log.error("Execution exception while creating node", e);
        }
        return node;
    }
}
