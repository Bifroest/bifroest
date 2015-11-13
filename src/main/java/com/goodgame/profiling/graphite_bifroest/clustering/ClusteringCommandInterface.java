package com.goodgame.profiling.graphite_bifroest.clustering;

import java.util.Collection;
import java.util.concurrent.Future;

import com.goodgame.profiling.bifroest.balancing.BucketMapping;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;

public interface ClusteringCommandInterface {
    void buildANewClusterSoon();
    void startClusteringSoon();
    void reactToJoinSoon( NodeMetadata joinedNodeMetadata );
    void reactToNewMappingSoon( BucketMapping<NodeMetadata> newMapping );
    void reactToNewLeaderSoon( NodeMetadata newLeader );
    void reactToLeaveSoon( NodeMetadata leavingNodeMetadata );
    void reactToTransferredMetricsSoon( Collection<String> metrics );
    Future<?> reactToShutdownSoon();
    void reactToAbandonedMetricsSoon( NodeMetadata leavingNodeMetadata, Collection<String> metrics );
}
