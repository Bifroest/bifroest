package io.bifroest.bifroest.commands;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.boot.interfaces.Environment;
import io.bifroest.bifroest.systems.BifroestIdentifiers;
import io.bifroest.commons.net.jsonserver.Command;

@MetaInfServices
public final class GetIntervalCommand< E extends Environment > implements Command<E> {
    @Override
    public String getJSONCommand() {
        return "get-interval";
    }

    @Override
    public List<Pair<String, Boolean>> getParameters() {
        return Collections.emptyList();
    }

    @Override
    public JSONObject execute( JSONObject input, E environment ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addRequirements( Collection<String> dependencies ) {
        dependencies.add( BifroestIdentifiers.REBUILDER );
    }
}
