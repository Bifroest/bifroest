package com.goodgame.profiling.graphite_bifroest.clustering.state;

import com.goodgame.profiling.bifroest.balancing.BucketMapping;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.ClusterState;
import com.goodgame.profiling.graphite_bifroest.clustering.ClusteringCommandInterface;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.graphite_bifroest.clustering.communication.MutableClusterCommunication;
import com.goodgame.profiling.graphite_bifroest.metric_cache.MetricCache;

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
