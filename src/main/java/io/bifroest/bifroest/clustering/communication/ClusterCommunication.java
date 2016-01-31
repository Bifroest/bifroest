package io.bifroest.bifroest.clustering.communication;

import java.util.Collection;

import org.json.JSONObject;

import io.bifroest.bifroest_client.metadata.NodeMetadata;

public interface ClusterCommunication {
    void sendToLeader( JSONObject message );

    void sendMetricsToNodes( Collection<String> metricsToSend );
    void informLeaderAboutLeavingAndAbandonedMetrics( NodeMetadata leavingNodeMetadata, Collection<String> abandonedMetrics );

    void broadcastNewBucketMapping();
    void broadcastNewLeader( NodeMetadata newLeader );
}
