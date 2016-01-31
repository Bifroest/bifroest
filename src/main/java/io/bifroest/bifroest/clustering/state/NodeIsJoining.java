package io.bifroest.bifroest.clustering.state;

import io.bifroest.balancing.BucketMapping;
import io.bifroest.bifroest_client.metadata.ClusterState;
import io.bifroest.bifroest_client.metadata.NodeMetadata;
import io.bifroest.bifroest.clustering.communication.ClusterCommunication;
import io.bifroest.bifroest.clustering.communication.MutableClusterCommunication;
import io.bifroest.bifroest.metric_cache.MetricCache;
import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NodeIsJoining extends AbstractNodeState {

    private static final Logger log = LogManager.getLogger();

    public NodeIsJoining() {
        super(KnownClusterStatePolicy.REJECT, MetricRequestPolicy.REJECT);
    }

    @Override
    public NodeState reactToNewMapping(ClusterState clusterState, NodeMetadata ownMetadata,
            MetricCache metricCache, BucketMapping<NodeMetadata> newMapping, ClusterCommunication communication)
            throws IOException {
        clusterState.setBucketMapping(newMapping);
        log.debug("Got new mapping, now I will switch to member state");
        return new NodeIsMember();
    }

    @Override
    public NodeState reactToNewLeader(ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata newLeaderMetadata)
            throws IOException {
        clusterState.setLeader(newLeaderMetadata);
        return this;
    }

    @Override
    public NodeState reactToJoin(ClusterState clusterState, NodeMetadata myOwnMetadata,
            NodeMetadata joinedNodeMetadata, ClusterCommunication communication, MetricCache metricCache )
            throws IOException {
        clusterState.addNode(joinedNodeMetadata);
        return this;
    }

    @Override
    public NodeState reactToLeave(ClusterState clusterState, NodeMetadata ownMetadata,
            NodeMetadata leavingNodeMetadata, MutableClusterCommunication communication)
            throws IOException {
        clusterState.removeNode(leavingNodeMetadata);
        communication.disconnectFrom(leavingNodeMetadata);
        log.debug("I am {}", ownMetadata);
        log.debug("Current leader: {}", clusterState.getLeader());
        log.debug("Leaving: {}", leavingNodeMetadata);
        log.debug("Potential new leader: {}", newLeader(clusterState));
        if (clusterState.getLeader().equals(leavingNodeMetadata)
                && newLeader(clusterState).equals(ownMetadata)) {
            clusterState.setLeader(ownMetadata);
            communication.broadcastNewLeader( ownMetadata );
            clusterState.getBucketMapping().leaveNode(leavingNodeMetadata);
            clusterState.getBucketMapping().joinNode(ownMetadata);
            communication.broadcastNewBucketMapping();
            return new NodeIsLeader();
        } else {
            return this;
        }
    }
}
