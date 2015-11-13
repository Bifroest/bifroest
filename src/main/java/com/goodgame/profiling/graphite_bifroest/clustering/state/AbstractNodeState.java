package com.goodgame.profiling.graphite_bifroest.clustering.state;

import java.io.IOException;
import java.util.Collection;

import com.goodgame.profiling.bifroest.balancing.BucketMapping;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.ClusterState;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.graphite_bifroest.clustering.ClusteringCommandInterface;
import com.goodgame.profiling.graphite_bifroest.clustering.communication.ClusterCommunication;
import com.goodgame.profiling.graphite_bifroest.clustering.communication.MutableClusterCommunication;
import com.goodgame.profiling.graphite_bifroest.metric_cache.MetricCache;

/**
 * Convenience always-throw-IllegalState behavior.
 */
public abstract class AbstractNodeState implements NodeState {
    protected static enum KnownClusterStatePolicy { ANSWER, REJECT }
    protected static enum MetricRequestPolicy { ANSWER, REJECT }

    private final KnownClusterStatePolicy getKnownClusterStatePolicy;
    private final MetricRequestPolicy metricRequestPolicy;

    protected AbstractNodeState( KnownClusterStatePolicy getKnownClusterStatePolicy, MetricRequestPolicy metricRequestPolicy ) {
        this.getKnownClusterStatePolicy = getKnownClusterStatePolicy;
        this.metricRequestPolicy = metricRequestPolicy;
    }

    @Override
    public NodeState startClustering( ClusteringCommandInterface commands, ClusterState state, MutableClusterCommunication communication, NodeMetadata ownMetadata ) throws IOException {
        throw new IllegalStateException( "Cannot startClustering in state " + this.getClass().getSimpleName() );
    }


    @Override
    public NodeState buildANewCluster( ClusterState clusterState, NodeMetadata myOwnMetadata ) {
        throw new IllegalStateException( "Cannot buildANewCluster in state " + this.getClass().getSimpleName() );
    }

    @Override
    public NodeState reactToJoin( ClusterState clusterState, NodeMetadata myOwnMetadata, NodeMetadata joinedNodeMetadata, ClusterCommunication communication, MetricCache metricCache ) throws IOException {
        throw new IllegalStateException( "Cannot reactToJoin in state " + this.getClass().getSimpleName() );
    }

    @Override
    public NodeState reactToLeave( ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata leavingNodeMetadata, MutableClusterCommunication communication ) throws IOException {
        throw new IllegalStateException( "Cannot reactToLeave in state " + this.getClass().getSimpleName() );
    }

    @Override
    public NodeState reactToNewMapping( ClusterState clusterState, NodeMetadata ownMetadata, MetricCache metricCache, BucketMapping<NodeMetadata> newMapping, ClusterCommunication communication ) throws IOException {
        throw new IllegalStateException( "Cannot reactToNewMapping in state " + this.getClass().getSimpleName() );
    }

    @Override
    public NodeState reactToNewLeader( ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata newLeaderMetadata ) throws IOException {
        throw new IllegalStateException( "Cannot reactToNewLeader in state " + this.getClass().getSimpleName() );
    }

    @Override
    public NodeState reactToTransferredMetrics( MetricCache metricCache, Collection<String> metrics ) {
        throw new IllegalStateException( "Cannot reactToTransferredMetrics in state " + this.getClass().getSimpleName() );
    }

    @Override
    public NodeState reactToShutdown( ClusterState clusterState, NodeMetadata ownMetadata, MetricCache metricCache, ClusterCommunication communication ) throws IOException {
        throw new IllegalStateException( "Cannot reactToShutdown in state " + this.getClass().getSimpleName() );
    }

    @Override
    public NodeState reactToAbandonedMetrics( ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata leavingNodeMetadata,
                                              MutableClusterCommunication communication, MetricCache metricCache,
                                              Collection<String> abandonedMetrics ) throws IOException {
        throw new IllegalStateException( "Cannot reactToAbandonedMetrics in state " + this.getClass().getSimpleName() );
    }

    @Override
    public boolean doIAnswerGetClusterState() {
        return getKnownClusterStatePolicy == KnownClusterStatePolicy.ANSWER;
    }

    @Override
    public boolean doIAnswerGetMetrics() {
        return metricRequestPolicy == MetricRequestPolicy.ANSWER;
    }

    @Override
    public boolean doIAnswerGetMetricSets() {
        return metricRequestPolicy == MetricRequestPolicy.ANSWER;
    }

    @Override
    public boolean doIAnswerGetSubmetrics() {
        return metricRequestPolicy == MetricRequestPolicy.ANSWER;
    }

    @Override
    public boolean doIPreloadMetric( String metricName ) {
        throw new IllegalStateException( "Cannot answer to doIPreloadMetric in state " + this.getClass().getSimpleName() );
    }

    protected NodeMetadata newLeader( ClusterState clusterState ) {
        return clusterState.getKnownNodes()
                           .stream()
                           .min( NodeMetadata.leaderOrder )
                           .get(); // Calling .get() here is safe, because we never call that method when the cluster is empty
    }
}
