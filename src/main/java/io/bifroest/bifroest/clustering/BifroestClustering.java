package com.goodgame.profiling.graphite_bifroest.clustering;

import java.io.IOException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.goodgame.profiling.bifroest.balancing.BucketMapping;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.ClusterState;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.PortMap;
import com.goodgame.profiling.bifroest.bifroest_client.seeds.HostPortPair;
import com.goodgame.profiling.graphite_bifroest.clustering.communication.MutableClusterCommunication;
import com.goodgame.profiling.graphite_bifroest.clustering.communication.SocketBasedClusterCommunication;
import com.goodgame.profiling.graphite_bifroest.clustering.state.NodeIsLonelyAndSad;
import com.goodgame.profiling.graphite_bifroest.clustering.state.NodeState;
import com.goodgame.profiling.graphite_bifroest.clustering.statistics.NodeStateChangedEvent;
import com.goodgame.profiling.graphite_bifroest.metric_cache.MetricCache;


/**
 * This class is a container for the different parts of the clustering.
 * You can keep a reference of this class around after taking it out of
 * the environment. The same is not true for parts of this class.
 */
public class BifroestClustering implements ClusteringCommandInterface {
    private static final Logger log = LogManager.getLogger();

    private final ClusterState knownClusterState;
    private final MutableClusterCommunication communication;
    private final MetricCache metricCache;
    private final NodeMetadata myOwnMetadata;

    // all modifications to the cluster state and the node state
    // happen in this single threaded executor service, to avoid
    // concurrency in complex code.
    private final ExecutorService clusterBrain;

    // This is a volatile variable so different threads (for example the
    // threads in the multi server system) can see changes in the node state
    private volatile NodeState myOwnState;

    public BifroestClustering( List<HostPortPair> seeds, PortMap internalPortmap, MetricCache metricCache, Duration pingFrequency ) throws IOException {
        this.myOwnMetadata = NodeMetadata.fromAddress( InetAddress.getLocalHost().getCanonicalHostName(), internalPortmap );

        this.knownClusterState = new ClusterState();
        this.communication = new SocketBasedClusterCommunication( this, knownClusterState, myOwnMetadata, pingFrequency );
        this.metricCache = metricCache;

        this.clusterBrain = Executors.newFixedThreadPool( 1,
                                                          new BasicThreadFactory.Builder()
                                                                                .namingPattern( "ClusterBrain-%d" )
                                                                                .build());
        myOwnState = new NodeIsLonelyAndSad( seeds, metricCache );
        NodeStateChangedEvent.fire( myOwnState );
    }

    public Collection<NodeMetadata> getKnownNodes() {
        return knownClusterState.getKnownNodes();
    }

    public NodeMetadata getMyOwnMetadata() {
        return myOwnMetadata;
    }

    @FunctionalInterface
    private interface StateChange {
        public NodeState execute() throws Exception;
    }

    private Future<?> executeStateChangeSoon( StateChange stateChange, String transition ) {
        return clusterBrain.submit( () -> {
            try {
                NodeState oldState = myOwnState;
                NodeState newState = myOwnState;
                newState = stateChange.execute();
                log.info( "{} -> State change: {} -> {}", transition, oldState.getClass().getSimpleName(), newState.getClass().getSimpleName() );
                NodeStateChangedEvent.fire( newState );
                myOwnState = newState;
            } catch ( Exception e ) {
                log.warn( "Error while " + transition, e );
            }
        });
    }

    @Override
    public void startClusteringSoon() {
        executeStateChangeSoon(
                () -> myOwnState.startClustering( this, knownClusterState, communication, myOwnMetadata ),
                "start clustering" );
    }

    @Override
    public void buildANewClusterSoon() {
        executeStateChangeSoon(
                () -> myOwnState.buildANewCluster( knownClusterState, myOwnMetadata ),
                "building a new cluster" );
    }

    @Override
    public void reactToJoinSoon( NodeMetadata joinedNodeMetadata ) {
        executeStateChangeSoon(
                () -> myOwnState.reactToJoin( knownClusterState, myOwnMetadata, joinedNodeMetadata, communication, metricCache ),
                "reacting to join" );
    }

    @Override
    public void reactToNewLeaderSoon( NodeMetadata newLeaderMetadata ) {
        executeStateChangeSoon(
                () -> myOwnState.reactToNewLeader( knownClusterState, myOwnMetadata, newLeaderMetadata ),
                "reacting to new leader" );
    }

    @Override
    public void reactToLeaveSoon( NodeMetadata leavingNodeMetadata ) {
        if ( clusterBrain.isShutdown() ) {
            // The cluster brain is already dead, so this cannot possibly work
            // This is needed, so that the BifroestRemote.ReaderThread can handle the remote disconnecting in response to our shutdown.
            return;
        }
        executeStateChangeSoon(
                () -> myOwnState.reactToLeave( knownClusterState, myOwnMetadata, leavingNodeMetadata, communication ),
                "reacting to leave" );
    }

    @Override
    public void reactToNewMappingSoon( BucketMapping<NodeMetadata> newMapping ) {
        executeStateChangeSoon(
                () -> myOwnState.reactToNewMapping( knownClusterState, myOwnMetadata, metricCache, newMapping, communication ),
                "reacting to new mapping" );
    }

    @Override
    public void reactToTransferredMetricsSoon( Collection<String> metrics ) {
        executeStateChangeSoon(
                () -> myOwnState.reactToTransferredMetrics( metricCache, metrics ),
                "reacting to transferred metrics" );
    }

    @Override
    public Future<?> reactToShutdownSoon() {
        return executeStateChangeSoon(
                () -> {
                    return myOwnState.reactToShutdown( knownClusterState, myOwnMetadata, metricCache, communication );
                },
                "reacting to shutdown" );
    }

    @Override
    public void reactToAbandonedMetricsSoon( NodeMetadata leavingNodeMetadata, Collection<String> abandonedMetrics ) {
        executeStateChangeSoon(
                () -> myOwnState.reactToAbandonedMetrics( knownClusterState, myOwnMetadata, leavingNodeMetadata, communication, metricCache, abandonedMetrics ),
                "reacting to abandoned metrics" );
    }

    public ClusterState getClusterState() {
        return knownClusterState;
    }

    public void shutdown() {
        try {
            reactToShutdownSoon().get();
        } catch ( InterruptedException | ExecutionException ex ) {
            log.info( "Exception while waiting for clusterbrain ", ex );
        }
        clusterBrain.shutdown();
        communication.shutdown();

        try {
            clusterBrain.awaitTermination( Integer.MAX_VALUE, TimeUnit.DAYS );
        } catch ( InterruptedException ex ) {
            log.info( "Exception while shutting down clusterbrain ", ex );
        }
    }

    public boolean doIAnswerGetKnownClusterState() {
        return myOwnState.doIAnswerGetClusterState();
    }

    public boolean doIAnswerGetMetrics() {
        return myOwnState.doIAnswerGetMetrics();
    }

    public boolean doIAnswerGetSubMetrics() {
        return myOwnState.doIAnswerGetSubmetrics();
    }

    public boolean doIAnswerGetMetricSets() {
        return myOwnState.doIAnswerGetMetricSets();
    }

    public boolean doIPreloadMetric( String metricName ) {
        return myOwnState.doIPreloadMetric( metricName );
    }

    public NodeMetadata whichNodeCachesMetric( String name ) {
        return knownClusterState.getBucketMapping().getNodeFor( name.hashCode() );
    }
}