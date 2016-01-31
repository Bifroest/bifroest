package com.goodgame.profiling.graphite_bifroest.clustering.communication;

import com.goodgame.profiling.bifroest.balancing.BucketMapping;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;

public interface ClientCommunication {
    void disconnectClient( ClientRemote who );
    void broadcastNewNodeToClients( NodeMetadata newNodeMetadata );
    void broadcastNewMappingToClients( BucketMapping<NodeMetadata> newMapping );
}
