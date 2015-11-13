package com.goodgame.profiling.graphite_bifroest.clustering.state;

import com.goodgame.profiling.bifroest.balancing.BucketMapping;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.ClusterState;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.MappingFactory;
import com.goodgame.profiling.graphite_bifroest.metric_cache.MetricCache;

/**
 * In this state, the current bifroest node has not found any other
 * bifroest nodes and needs to build it's own cluster.
 *
 * TODO: Think about reject and reject in super-call
 * TODO: Implement stuff here, but that's another card on the wall
 */
public class NodeIsBuildingOwnCluster extends AbstractNodeState {
    
    private final MetricCache metricCache;
    
    public NodeIsBuildingOwnCluster(MetricCache metricCache) {
        super( KnownClusterStatePolicy.REJECT, MetricRequestPolicy.REJECT );
        this.metricCache = metricCache;
    }

    @Override
    public NodeState buildANewCluster( ClusterState clusterState, NodeMetadata ownMetadata ) {
        clusterState.addNode( ownMetadata );

        clusterState.setLeader( ownMetadata );

        BucketMapping<NodeMetadata> newBucketMapping = MappingFactory.newMapping( ownMetadata );
        clusterState.setBucketMapping( newBucketMapping );

        metricCache.loadSavedMetrics();
        
        return new NodeIsLeader();
    }
}
