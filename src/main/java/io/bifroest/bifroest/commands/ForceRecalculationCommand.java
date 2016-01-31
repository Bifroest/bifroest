package io.bifroest.bifroest.commands;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.bifroest.BifroestIdentifiers;
import io.bifroest.bifroest.rebuilder.EnvironmentWithTreeRebuilder;
import io.bifroest.commons.net.jsonserver.Command;

@MetaInfServices
public final class ForceRecalculationCommand< E extends EnvironmentWithTreeRebuilder > implements Command<E> {
    @Override
    public String getJSONCommand() {
        return "force-recalculation";
    }

    @Override
    public List<Pair<String, Boolean>> getParameters() {
        return Collections.emptyList();
    }

    @Override
    public JSONObject execute( JSONObject input, E environment ) {
        environment.getRebuilder().rebuild();
        return new JSONObject();
    }

    @Override
    public void addRequirements( Collection<String> dependencies ) {
        dependencies.add( BifroestIdentifiers.REBUILDER );
    }
}
