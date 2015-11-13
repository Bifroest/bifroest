package com.goodgame.profiling.graphite_bifroest.clustering.state;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.goodgame.profiling.bifroest.balancing.Bucket;
import com.goodgame.profiling.bifroest.balancing.BucketMapping;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.ClusterState;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.graphite_bifroest.clustering.communication.ClusterCommunication;
import com.goodgame.profiling.graphite_bifroest.clustering.communication.MutableClusterCommunication;
import com.goodgame.profiling.graphite_bifroest.metric_cache.MetricCache;

public final class NodeIsMember extends AbstractNodeState {
    private static final Logger log = LogManager.getLogger();
    private Map<String, Future<?>> preloadingMetricsMap;

    public NodeIsMember() {
        super( KnownClusterStatePolicy.ANSWER, MetricRequestPolicy.ANSWER );
        this.preloadingMetricsMap = new HashMap<>();
    }

    public NodeIsMember( Map<String, Future<?>> preloadingMetricsMap ) {
        super( KnownClusterStatePolicy.ANSWER, MetricRequestPolicy.ANSWER );
        this.preloadingMetricsMap = preloadingMetricsMap;
    }

    @Override
    public NodeState reactToJoin( ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata joinedNodeMetadata, ClusterCommunication communication, MetricCache metricCache ) {
        log.trace( "This node reacts to join of new node {}", joinedNodeMetadata.toString() );
        clusterState.addNode( joinedNodeMetadata );
        return this;
    }

    @Override
    public NodeState reactToLeave( ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata leavingNodeMetadata, MutableClusterCommunication communication ) throws IOException {
        log.trace( "This node reacts to leave of node {}", leavingNodeMetadata.toString() );
        clusterState.removeNode( leavingNodeMetadata );
        communication.disconnectFrom( leavingNodeMetadata );

        log.debug( "I am {}", ownMetadata );
        log.debug( "Current leader: {}", clusterState.getLeader() );
        log.debug( "Leaving: {}", leavingNodeMetadata );
        log.debug( "Potential new leader: {}", newLeader( clusterState ) );
        if ( clusterState.getLeader().equals( leavingNodeMetadata ) &&
                newLeader( clusterState ).equals( ownMetadata ) ) {
            clusterState.setLeader( ownMetadata );
            communication.broadcastNewLeader( ownMetadata );

            clusterState.getBucketMapping().leaveNode( leavingNodeMetadata );
            communication.broadcastNewBucketMapping();
            log.trace( "Node becomes the new leader!" );
            return new NodeIsLeader();
        } else {
            return this;
        }
    }

    @Override
    public NodeState reactToNewMapping( ClusterState clusterState, NodeMetadata ownMetadata,
                                        MetricCache metricCache, BucketMapping<NodeMetadata> newMapping,
                                        ClusterCommunication communication ) throws IOException {
        log.trace( "Node reacts to new Mapping" );
        Objects.requireNonNull( clusterState, "clusterState" );
        Objects.requireNonNull( ownMetadata, "ownMetadata" );
        Objects.requireNonNull( metricCache, "metricCache" );
        Objects.requireNonNull( newMapping, "newMapping" );
        Objects.requireNonNull( communication, "communication" );

        if ( clusterState.getBucketMapping() != null ) {
            clusterState.setBucketMapping( newMapping );
            Bucket<NodeMetadata> newBucket = Objects.requireNonNull( newMapping.getBucketFor( ownMetadata ), "newBucket" );

            log.trace( "Looking for metrics to transfer to other nodes" );
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
        }
        return this;
    }

    @Override
    public NodeState reactToNewLeader( ClusterState clusterState, NodeMetadata ownMetadata, NodeMetadata newLeaderMetadata ) throws IOException {
        log.trace( "Node reatcs to new leader" );
        log.debug( "I am {}", ownMetadata );
        log.debug( "Current leader: {}", clusterState.getLeader() );
        log.debug( "New leader was broadcasted: {}", newLeaderMetadata );
        clusterState.setLeader( newLeaderMetadata );

        if ( ownMetadata.equals( newLeaderMetadata ) ) {
            log.debug( "I am the new leader! Muhahaha..." );
            return new NodeIsLeader( preloadingMetricsMap );
        }
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
        communication.informLeaderAboutLeavingAndAbandonedMetrics( ownMetadata, metricCache.getAllMetricNames() );
        return new NodeIsLeaving();
    }

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
