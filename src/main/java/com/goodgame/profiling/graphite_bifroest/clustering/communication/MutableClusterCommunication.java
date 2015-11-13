package com.goodgame.profiling.graphite_bifroest.clustering.communication;

import java.io.IOException;
import java.util.Collection;

import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;

public interface MutableClusterCommunication extends ClusterCommunication {
    void connectAndIntroduceTo( NodeMetadata newNode ) throws IOException;
    void disconnectFrom( NodeMetadata leavingNode ) throws IOException;
    void shutdown();

    default void connectAndIntroduceToAll( Collection<NodeMetadata> allNodes ) throws IOException {
        for ( NodeMetadata node : allNodes ) {
            connectAndIntroduceTo( node );
        }
    }
}
