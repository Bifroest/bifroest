package io.bifroest.bifroest.commands;

import io.bifroest.commons.statistics.ProgramStateChanged;
import io.bifroest.commons.statistics.SimpleProgramStateTracker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.bifroest.BifroestIdentifiers;
import io.bifroest.bifroest.prefixtree.EnvironmentWithPrefixTree;
import io.bifroest.commons.configuration.EnvironmentWithJSONConfiguration;
import io.bifroest.commons.net.jsonserver.Command;

@MetaInfServices
public class GetAgeCommand< E extends EnvironmentWithJSONConfiguration & EnvironmentWithPrefixTree > implements Command<E> {
    private static final Logger log = LogManager.getLogger();

    public GetAgeCommand() {
        SimpleProgramStateTracker.forContext("bifroest-bifroest-getAgeCommand").storingIn("commandExecution.get-metric-age.stage-timing").build();
    }


    @Override
    public String getJSONCommand() {
        return "get-metric-age";
    }

    @Override
    public List<Pair<String, Boolean>> getParameters() {
        return Collections.<Pair<String, Boolean>> singletonList( new ImmutablePair<>( "metric-prefix", true ) );
    }

    @Override
    public JSONObject execute( JSONObject input, E environment ) {
        log.entry( input, environment );

        try {
            ProgramStateChanged.fireContextChangeToState("bifroest-bifroest-getAgeCommand", "StartGetMetricAge");
            List<String> blacklist = new ArrayList<>();
            environment.getConfigurationLoader().loadConfiguration();
            JSONObject config = environment.getConfiguration().getJSONObject( "bifroest" );
            JSONArray blackarray = config.getJSONArray( "blacklist" );
            for ( int i = 0; i < blackarray.length(); i++ ) {
                blacklist.add( blackarray.getString( i ) );
            }

            String prefix = input.getString( "metric-prefix" );
            ProgramStateChanged.fireContextChangeToState("bifroest-bifroest-getAgeCommand", "TreeFindAge");
            long age = environment.getTree().findAge( prefix, blacklist );
            log.trace( "Prefix {}, Blacklist {}, age {}", prefix, blacklist, age );

            // Future timestamp is bad, and PrefixTree defaults to Long.MAX_VALUE
            if ( age > System.currentTimeMillis() / 1000 ) {
                return log.exit( new JSONObject().put( "found", false ) );
            } else {
                return log.exit( new JSONObject().put( "found", true ).put( "age", age ) );
            }
        } finally {
            ProgramStateChanged.fireContextStopped("bifroest-bifroest-getAgeCommand");
        }
    }

    @Override
    public void addRequirements( Collection<String> dependencies ) {
        dependencies.add( BifroestIdentifiers.REBUILDER );
    }
}
