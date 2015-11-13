package com.goodgame.profiling.graphite_bifroest.clustering.state;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.goodgame.profiling.bifroest.balancing.BucketMapping;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.ClusterState;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.bifroest.bifroest_client.seeds.HostPortPair;
import com.goodgame.profiling.bifroest.bifroest_client.seeds.KnownClusterStateFromMultiServer;
import com.goodgame.profiling.bifroest.bifroest_client.seeds.KnownClusterStateRequester;
import com.goodgame.profiling.graphite_bifroest.clustering.ClusteringCommandInterface;
import com.goodgame.profiling.graphite_bifroest.clustering.communication.MutableClusterCommunication;
import com.goodgame.profiling.graphite_bifroest.metric_cache.MetricCache;

/**
 * This state is before a node even tried to get find other
 * cluster nodes. In this state, the node needs to think hard
 * how to get into a cluster, even if that means creating a
 * new cluster.
 */
public class NodeIsLonelyAndSad extends AbstractNodeState {
    private static final Logger log = LogManager.getLogger();

    private final KnownClusterStateRequester nodeRequester;
    private final List<HostPortPair> seeds;
    private final MetricCache metricCache;
    
    public NodeIsLonelyAndSad( List<HostPortPair> bifroestSeeds, MetricCache metricCache ) {
        this(new KnownClusterStateFromMultiServer(), bifroestSeeds, metricCache );        
    }

    // Test Constructor
    public NodeIsLonelyAndSad( KnownClusterStateRequester nodeRequester, 
            List<HostPortPair> bifroestSeeds, MetricCache metricCache ) {
        super( KnownClusterStatePolicy.REJECT, MetricRequestPolicy.REJECT );
        this.nodeRequester = nodeRequester;
        this.seeds = bifroestSeeds;
        this.metricCache = metricCache;
    }

    @Override
    public NodeState startClustering( ClusteringCommandInterface clusteringCommands,
                                      ClusterState clusterState,
                                      MutableClusterCommunication communication,
                                      NodeMetadata ownMetadata ) throws IOException {
        for ( HostPortPair seed : seeds ) {
            Optional<ClusterState> alreadyKnownClusterState = nodeRequester.request( seed );
            
            if ( alreadyKnownClusterState.isPresent() ) {
                log.info( "Found ClusterState {}", alreadyKnownClusterState );
                Collection<NodeMetadata> alreadyKnownNodes = alreadyKnownClusterState.get().getKnownNodes();
                NodeMetadata leader = alreadyKnownClusterState.get().getLeader();
                BucketMapping<NodeMetadata> mapping = alreadyKnownClusterState.get().getBucketMapping();

                communication.connectAndIntroduceToAll( alreadyKnownNodes );
                clusterState.addAll( alreadyKnownNodes );
                clusterState.addNode( ownMetadata );
                clusterState.setLeader( leader );
                clusterState.setBucketMapping( mapping );

                return new NodeIsJoining();
            }
        }
        clusteringCommands.buildANewClusterSoon();
        return new NodeIsBuildingOwnCluster(metricCache);
    }
}
