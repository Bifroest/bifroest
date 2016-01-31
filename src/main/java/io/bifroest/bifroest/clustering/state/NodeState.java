package io.bifroest.bifroest.clustering.state;

import java.io.IOException;
import java.util.Collection;

import io.bifroest.balancing.BucketMapping;
import io.bifroest.bifroest_client.metadata.ClusterState;
import io.bifroest.bifroest_client.metadata.NodeMetadata;
import io.bifroest.bifroest.clustering.ClusteringCommandInterface;
import io.bifroest.bifroest.clustering.communication.ClusterCommunication;
import io.bifroest.bifroest.clustering.communication.MutableClusterCommunication;
import io.bifroest.bifroest.metric_cache.MetricCache;

public interface NodeState {
    // when extending this interface, prefer passing in the necessary
    // state via parameters over keeping attributes in the various
    // node state classes.
    // This makes it easier to extend this interface, instead of having to
    // shovel some state variable across a dozen different state classes.

    /**
     * This is the first method called when bifroest starts. It is
     * responsible for finding the current nodes in the cluster.
     */
    NodeState startClustering( ClusteringCommandInterface commands, ClusterState clusterState, MutableClusterCommunication communication, NodeMetadata ownMetadata ) throws IOException;

    NodeState buildANewCluster( ClusterState clusterState, NodeMetadata ownMetadata );
    NodeState reactToJoin( ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata joinedNodeMetadata, ClusterCommunication communication, MetricCache metricCache ) throws IOException;
    NodeState reactToLeave( ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata leavingNodeMetadata, MutableClusterCommunication communication ) throws IOException;
    NodeState reactToNewMapping( ClusterState clusterState, NodeMetadata ownMetadata, MetricCache metricCache, BucketMapping<NodeMetadata> newMapping, ClusterCommunication communication ) throws IOException;
    NodeState reactToNewLeader( ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata newLeaderMetadata ) throws IOException;
    NodeState reactToTransferredMetrics( MetricCache metricCache, Collection<String> metrics );
    NodeState reactToShutdown( ClusterState clusterState, NodeMetadata ownMetadata, MetricCache metricCache, ClusterCommunication communication ) throws IOException;
    NodeState reactToAbandonedMetrics( ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata leavingNodeMetadata, MutableClusterCommunication communication, MetricCache metricCache, Collection<String> abandonedMetrics ) throws IOException;

    boolean doIAnswerGetClusterState();
    boolean doIAnswerGetMetrics();
    boolean doIAnswerGetMetricSets();
    boolean doIAnswerGetSubmetrics();

    boolean doIPreloadMetric( String metricName );
}
