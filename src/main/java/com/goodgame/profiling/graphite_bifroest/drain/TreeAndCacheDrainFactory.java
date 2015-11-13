package com.goodgame.profiling.graphite_bifroest.drain;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.drains.BasicDrainFactory;
import com.goodgame.profiling.drains.Drain;
import com.goodgame.profiling.graphite_bifroest.systems.BifroestEnvironment;
import com.goodgame.profiling.graphite_bifroest.systems.BifroestIdentifiers;

@MetaInfServices
public class TreeAndCacheDrainFactory implements BasicDrainFactory<BifroestEnvironment> {

    @Override
    public List<Class<? super BifroestEnvironment>> getRequiredEnvironments() {
        return Collections.singletonList(BifroestEnvironment.class);
    }
    
    @Override
    public void addRequiredSystems( Collection<String> requiredSystems, JSONObject subconfiguration ){
        requiredSystems.add(BifroestIdentifiers.REBUILDER);
        requiredSystems.add(BifroestIdentifiers.METRIC_CACHE);
    }

    @Override
    public String handledType() {
        return "tree-and-cache";
    }

    @Override
    public Drain create(BifroestEnvironment environment, JSONObject subconfiguration) {
        return new TreeAndCacheDrain(environment);
    }

}
