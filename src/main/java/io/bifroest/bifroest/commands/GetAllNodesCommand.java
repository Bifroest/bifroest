package io.bifroest.bifroest.commands;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.bifroest.BifroestIdentifiers;
import io.bifroest.bifroest.prefixtree.EnvironmentWithPrefixTree;
import io.bifroest.commons.net.jsonserver.Command;

@MetaInfServices
public class GetAllNodesCommand< E extends EnvironmentWithPrefixTree > implements Command<E> {
    @Override
    public String getJSONCommand() {
        return "get-all-nodes";
    }

    @Override
    public List<Pair<String, Boolean>> getParameters() {
        return Collections.emptyList();
    }

    @Override
    public JSONObject execute( JSONObject input, E environment ) {
        final JSONArray names = new JSONArray();

        environment.getTree().forAllLeaves(
            ( name, age ) -> {
                if( names.length() < 1000 ) {
                    names.put( name );
                }
            } );

        JSONObject result = new JSONObject().put( "result", names );

        result.put( "there-are-more-results", names.length() >= 1000 );

        return result;
    }

    @Override
    public void addRequirements( Collection<String> dependencies ) {
        dependencies.add( BifroestIdentifiers.REBUILDER );
    }
}
