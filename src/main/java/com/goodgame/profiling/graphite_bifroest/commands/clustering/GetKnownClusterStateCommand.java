package com.goodgame.profiling.graphite_bifroest.commands.clustering;

import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.bifroest.bifroest_client.metadata.ClusterState;
import com.goodgame.profiling.commons.systems.net.jsonserver.Command;
import com.goodgame.profiling.graphite_bifroest.clustering.EnvironmentWithClustering;

@MetaInfServices
public class GetKnownClusterStateCommand<E extends EnvironmentWithClustering> implements Command<E> {

    @Override
    public String getJSONCommand() {
        return "get-known-cluster-state";
    }

    @Override
    public List<Pair<String, Boolean>> getParameters() {
        return Collections.emptyList();
    }

    @Override
    public JSONObject execute( JSONObject input, E environment ) {
        if ( !environment.getClustering().doIAnswerGetKnownClusterState() ) {
            return new JSONObject().put( "answered", false );
        }
        return new JSONObject().put( "answered", true )
                               .put( "cluster-state", environment.getClustering().getClusterState().toJSON() );
    }

    public static ClusterState decodeFromJSON( JSONObject clusterState ) {
        return ClusterState.fromJSON( clusterState );
    }
}
