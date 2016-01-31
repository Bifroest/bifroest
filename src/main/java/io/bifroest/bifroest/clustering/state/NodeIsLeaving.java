package com.goodgame.profiling.graphite_bifroest.clustering.state;

import java.io.IOException;

import com.goodgame.profiling.bifroest.bifroest_client.metadata.ClusterState;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.graphite_bifroest.clustering.communication.MutableClusterCommunication;

public class NodeIsLeaving extends AbstractNodeState {
    public NodeIsLeaving() {
        super( KnownClusterStatePolicy.REJECT, MetricRequestPolicy.REJECT );
    }

    @Override
    public NodeState reactToLeave(ClusterState clusterState,
            NodeMetadata ownMetadata, NodeMetadata leavingNodeMetadata,
            MutableClusterCommunication communication) throws IOException {
        // intentionally empty
        // When we shutdown ourselves, the other side closes the connection
        // And this is seen as the other side leaving.

        return this;
    }
}
