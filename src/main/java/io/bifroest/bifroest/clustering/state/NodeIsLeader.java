package io.bifroest.bifroest.clustering.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.bifroest.balancing.Bucket;
import io.bifroest.bifroest_client.metadata.ClusterState;
import io.bifroest.bifroest_client.metadata.NodeMetadata;
import io.bifroest.bifroest.clustering.communication.ClusterCommunication;
import io.bifroest.bifroest.clustering.communication.MutableClusterCommunication;
import io.bifroest.bifroest.metric_cache.MetricCache;

public final class NodeIsLeader extends AbstractNodeState {
    private static final Logger log = LogManager.getLogger();
    private final Map<String, Future<?>> preloadingMetricsMap;

    public NodeIsLeader() {
        super( KnownClusterStatePolicy.ANSWER, MetricRequestPolicy.ANSWER );
        this.preloadingMetricsMap = new HashMap<>();
    }

    public NodeIsLeader( Map<String, Future<?>> preloadingMetricsMap ) {
        super( KnownClusterStatePolicy.ANSWER, MetricRequestPolicy.ANSWER );
        this.preloadingMetricsMap = preloadingMetricsMap;
    }

    @Override
    public NodeState reactToJoin( ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata joinedNodeMetadata, ClusterCommunication communication, MetricCache metricCache ) throws IOException {
        log.trace( "This node reacts to join of new node {}", joinedNodeMetadata.toString() );
        clusterState.addNode( joinedNodeMetadata );
        clusterState.getBucketMapping().joinNode( joinedNodeMetadata );

        communication.broadcastNewBucketMapping();

        log.trace( "Looking for metrics to transfer to other nodes" );
        Bucket<NodeMetadata> newBucket = clusterState.getBucketMapping().getBucketFor( ownMetadata );
        ArrayList<String> metricsToTransfer = new ArrayList<>();
        List<String> mostRequestedMetrics = metricCache.getMostRequestedMetrics();
        log.trace( "Got a number of {} most requested metrics", mostRequestedMetrics.size() );
        for ( String metricName : mostRequestedMetrics ) {
            int metricHash = metricName.hashCode();
            if( ! newBucket.contains( metricHash ) ) {
                metricsToTransfer.add( metricName );
                metricCache.triggerEviction( metricName );
            }
        }
        log.trace( "Have to transfer {} metrics", metricsToTransfer.size() );
        communication.sendMetricsToNodes( metricsToTransfer );

        return this;
    }

    @Override
    public NodeState reactToLeave( ClusterState clusterState, NodeMetadata ownMetadata,
                                   NodeMetadata leavingNodeMetadata, MutableClusterCommunication communication ) throws IOException {
        log.trace( "This node reacts to leave of node {}", leavingNodeMetadata.toString() );
        if ( ! clusterState.getKnownNodes().contains( leavingNodeMetadata ) ) {
            log.trace( "Leaving node was already removed from ClusterState. Will do nothing" );
            return this;
        }

        clusterState.removeNode( leavingNodeMetadata );
        communication.disconnectFrom( leavingNodeMetadata );
        clusterState.getBucketMapping().leaveNode( leavingNodeMetadata );

        communication.broadcastNewBucketMapping();

        return this;
    }

    @Override
    public NodeState reactToNewLeader( ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata newLeaderMetadata ) throws IOException {
        log.trace( "This node reacts to new leader and becomes a Member node. New leader is {}", newLeaderMetadata );
        clusterState.setLeader( newLeaderMetadata );
        return new NodeIsMember();
    }

    @Override
    public NodeState reactToAbandonedMetrics( ClusterState clusterState, NodeMetadata ownMetadata, 
                                              NodeMetadata leavingNodeMetadata, MutableClusterCommunication communication,
                                              MetricCache metricCache, Collection<String> abandonedMetrics ) throws IOException {
        log.trace( "Node reacts to {} abandoned metrics", abandonedMetrics.size() );
        reactToLeave( clusterState, ownMetadata, leavingNodeMetadata, communication );

        // care about Abandoned Metrics
        List<String> metricsToTransfer = new ArrayList<>();
        List<String> myOwnMetrics = new ArrayList<>();
        for ( String metricName : abandonedMetrics ) {
            NodeMetadata responsibleNode = clusterState.getBucketMapping().getNodeFor( metricName.hashCode() );
            if ( responsibleNode.equals( ownMetadata ) ){
                myOwnMetrics.add( metricName );
            } else {
                metricsToTransfer.add( metricName );
            }
        }
        log.trace( "This node will preload {} and transfer {} of the abandoned metrics", myOwnMetrics.size(), metricsToTransfer.size() );
        preloadingMetricsMap.putAll( metricCache.preloadMetrics( myOwnMetrics ) );
        communication.sendMetricsToNodes( metricsToTransfer );

        return this;
    }

    @Override
    public NodeState reactToTransferredMetrics( MetricCache metricCache, Collection<String> metrics ) {
        log.trace( "Node reacts to and preloads {} transferred metrics", metrics.size() );
        preloadingMetricsMap.putAll( metricCache.preloadMetrics( metrics ) );
        return this;
    }

    @Override
    public NodeState reactToShutdown( ClusterState clusterState, NodeMetadata ownMetadata, MetricCache metricCache, ClusterCommunication communication ) throws IOException {
        log.trace( "Node reacts to own shutdown" );
        clusterState.removeNode( ownMetadata );
        if ( clusterState.getKnownNodes().isEmpty() ) {
            log.info( "There is no other node left" );
            return new NodeIsLeaving();
        }
        NodeMetadata newLeader = newLeader( clusterState );

        log.debug( "I am {}", ownMetadata );
        log.debug( "Current leader: {}", clusterState.getLeader() );
        log.debug( "Will broadcast potential new leader: {}", newLeader );

        clusterState.setLeader( newLeader );
        communication.broadcastNewLeader( newLeader );

        NodeState member = new NodeIsMember( preloadingMetricsMap );
        return member.reactToShutdown( clusterState, ownMetadata, metricCache, communication );
    }

    @Override
    public boolean doIPreloadMetric( String metricName ) {
        if ( ! preloadingMetricsMap.containsKey( metricName ) ) {
            return false;
        }
        boolean metricIsPreloaded = preloadingMetricsMap.get( metricName ).isDone();
        if ( metricIsPreloaded ) {
            preloadingMetricsMap.remove( metricName );
        }
        return ! metricIsPreloaded;
    }
}
