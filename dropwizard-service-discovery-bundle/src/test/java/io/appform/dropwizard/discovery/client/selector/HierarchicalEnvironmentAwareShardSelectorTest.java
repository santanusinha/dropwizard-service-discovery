package io.appform.dropwizard.discovery.client.selector;

import com.flipkart.ranger.finder.Service;
import com.flipkart.ranger.finder.sharded.MapBasedServiceRegistry;
import com.flipkart.ranger.model.ServiceNode;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import io.appform.dropwizard.discovery.common.ShardInfo;
import lombok.val;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;

public class HierarchicalEnvironmentAwareShardSelectorTest {


    private HierarchicalEnvironmentAwareShardSelector hierarchicalEnvironmentAwareShardSelector;

    @Mock
    private MapBasedServiceRegistry<ShardInfo> serviceRegistry;

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        this.hierarchicalEnvironmentAwareShardSelector = new HierarchicalEnvironmentAwareShardSelector();
    }

    @Test
    public void testNoNodeAvailableForTheEnvironment() {
        val serviceName = UUID.randomUUID().toString();
        val service = Mockito.mock(Service.class);
        doReturn(serviceName).when(service).getServiceName();
        doReturn(service).when(serviceRegistry).getService();

        ListMultimap<ShardInfo, ServiceNode<ShardInfo>> serviceNodes = ArrayListMultimap.create();
        serviceNodes.put(
                ShardInfo.builder().environment("x.y").build(),
                new ServiceNode<>("host1", 8888, new ShardInfo("x.y")));

        serviceNodes.put(
                ShardInfo.builder().environment("x").build(),
                new ServiceNode<>("host1", 9999, new ShardInfo("x")));

        doReturn(serviceNodes).when(serviceRegistry).nodes();

        val nodes = hierarchicalEnvironmentAwareShardSelector.nodes(
                ShardInfo.builder().environment("z").build(),
                serviceRegistry);
        assertEquals(0, nodes.size());
    }

    @Test
    public void testNodeAvailableForChildEnv() {
        val serviceName = UUID.randomUUID().toString();
        val service = Mockito.mock(Service.class);
        doReturn(serviceName).when(service).getServiceName();
        doReturn(service).when(serviceRegistry).getService();

        ListMultimap<ShardInfo, ServiceNode<ShardInfo>> serviceNodes = ArrayListMultimap.create();
        serviceNodes.put(
                ShardInfo.builder().environment("x.y").build(),
                new ServiceNode<>("host1", 8888, new ShardInfo("x.y")));

        serviceNodes.put(
                ShardInfo.builder().environment("x").build(),
                new ServiceNode<>("host2", 9999, new ShardInfo("x")));

        doReturn(serviceNodes).when(serviceRegistry).nodes();

        val nodes = hierarchicalEnvironmentAwareShardSelector.nodes(
                ShardInfo.builder().environment("x.y").build(),
                serviceRegistry);
        assertEquals(1, nodes.size());
        assertEquals("host1", nodes.get(0).getHost());
        assertEquals(8888, nodes.get(0).getPort());
    }

    @Test
    public void testNoNodeAvailableForChildEnvButAvailableForParentEnv() {
        val serviceName = UUID.randomUUID().toString();
        val service = Mockito.mock(Service.class);
        doReturn(serviceName).when(service).getServiceName();
        doReturn(service).when(serviceRegistry).getService();

        ListMultimap<ShardInfo, ServiceNode<ShardInfo>> serviceNodes = ArrayListMultimap.create();
        serviceNodes.put(
                ShardInfo.builder().environment("x.y.z").build(),
                new ServiceNode<>("host1", 8888, new ShardInfo("x.y")));

        serviceNodes.put(
                ShardInfo.builder().environment("x").build(),
                new ServiceNode<>("host2", 9999, new ShardInfo("x")));

        doReturn(serviceNodes).when(serviceRegistry).nodes();

        val nodes = hierarchicalEnvironmentAwareShardSelector.nodes(
                ShardInfo.builder().environment("x.y").build(),
                serviceRegistry);
        assertEquals(1, nodes.size());
        assertEquals("host2", nodes.get(0).getHost());
        assertEquals(9999, nodes.get(0).getPort());
    }

    @Test
    public void testAllNodes() {
        val serviceName = UUID.randomUUID().toString();
        val service = Mockito.mock(Service.class);
        doReturn(serviceName).when(service).getServiceName();
        doReturn(service).when(serviceRegistry).getService();

        ListMultimap<ShardInfo, ServiceNode<ShardInfo>> serviceNodes = ArrayListMultimap.create();
        serviceNodes.put(
                ShardInfo.builder().environment("x.y.z").build(),
                new ServiceNode<>("host1", 8888, new ShardInfo("x.y")));

        serviceNodes.put(
                ShardInfo.builder().environment("x").build(),
                new ServiceNode<>("host2", 9999, new ShardInfo("x")));

        doReturn(serviceNodes).when(serviceRegistry).nodes();

        val nodes = hierarchicalEnvironmentAwareShardSelector.nodes(
                ShardInfo.builder().environment("*").build(),
                serviceRegistry);
        assertEquals(2, nodes.size());
    }

}