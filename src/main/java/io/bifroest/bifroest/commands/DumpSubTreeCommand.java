package io.bifroest.bifroest.commands;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.bifroest.systems.BifroestIdentifiers;
import io.bifroest.bifroest.systems.prefixtree.EnvironmentWithPrefixTree;
import io.bifroest.bifroest.systems.prefixtree.PrefixTree;
import io.bifroest.commons.net.jsonserver.Command;

@MetaInfServices
public final class DumpSubTreeCommand< E extends EnvironmentWithPrefixTree > implements Command<E> {
    private static final Logger log = LogManager.getLogger();

    @SuppressWarnings("deprecation")
    // Not possible to handle reasonably.
    @Override
    public JSONObject execute( JSONObject input, E environment ) {
        String prefix = input.getString( "prefix" );

        log.info( "Someone used DumpSubTreeCommand. If bifroest explodes, you know why!" );

        return PrefixTree.toJSONObject( environment.getTree(), prefix );
    }

    @Override
    public String getJSONCommand() {
        return "dump-sub-tree";
    }

    @Override
    public List<Pair<String, Boolean>> getParameters() {
        return Collections.<Pair<String, Boolean>> singletonList( new ImmutablePair<>( "prefix", true ) );
    }

    @Override
    public void addRequirements( Collection<String> dependencies ) {
        dependencies.add( BifroestIdentifiers.REBUILDER );
    }
}
