package com.goodgame.profiling.graphite_bifroest.clustering.communication;

import java.util.Collection;

import org.json.JSONObject;

import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;

public interface ClusterCommunication {
    void sendToLeader( JSONObject message );

    void sendMetricsToNodes( Collection<String> metricsToSend );
    void informLeaderAboutLeavingAndAbandonedMetrics( NodeMetadata leavingNodeMetadata, Collection<String> abandonedMetrics );

    void broadcastNewBucketMapping();
    void broadcastNewLeader( NodeMetadata newLeader );
}
