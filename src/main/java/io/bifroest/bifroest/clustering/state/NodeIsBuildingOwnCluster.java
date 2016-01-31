package io.bifroest.bifroest.clustering.state;

import io.bifroest.balancing.BucketMapping;
import io.bifroest.bifroest_client.metadata.ClusterState;
import io.bifroest.bifroest_client.metadata.NodeMetadata;
import io.bifroest.bifroest_client.metadata.MappingFactory;
import io.bifroest.bifroest.metric_cache.MetricCache;

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
