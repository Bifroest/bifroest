package io.bifroest.bifroest.clustering.state;

import io.bifroest.balancing.BucketMapping;
import io.bifroest.bifroest_client.metadata.ClusterState;
import io.bifroest.bifroest.clustering.ClusteringCommandInterface;
import io.bifroest.bifroest_client.metadata.NodeMetadata;
import io.bifroest.bifroest.clustering.communication.MutableClusterCommunication;
import io.bifroest.bifroest.metric_cache.MetricCache;

import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public abstract class NodeStateTestFixture {
    @Mock protected ClusteringCommandInterface commands;
    @Mock protected MutableClusterCommunication communication;
    @Mock protected BucketMapping<NodeMetadata> mapping;
    @Mock protected MetricCache metricCache;

    protected ClusterState clusterState;

    @Before
    public void initFixture() {
        MockitoAnnotations.initMocks( this );
        clusterState = new ClusterState();
    }
}
