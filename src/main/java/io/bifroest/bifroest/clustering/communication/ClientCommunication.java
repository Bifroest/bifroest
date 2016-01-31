package io.bifroest.bifroest.clustering.communication;

import io.bifroest.balancing.BucketMapping;
import io.bifroest.bifroest_client.metadata.NodeMetadata;

public interface ClientCommunication {
    void disconnectClient( ClientRemote who );
    void broadcastNewNodeToClients( NodeMetadata newNodeMetadata );
    void broadcastNewMappingToClients( BucketMapping<NodeMetadata> newMapping );
}
