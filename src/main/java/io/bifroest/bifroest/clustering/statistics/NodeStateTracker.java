package com.goodgame.profiling.graphite_bifroest.clustering.statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.collections4.map.LazyMap;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.commons.statistics.WriteToStorageEvent;
import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;
import com.goodgame.profiling.commons.statistics.gathering.StatisticGatherer;
import com.goodgame.profiling.commons.statistics.storage.MetricStorage;

@MetaInfServices
public final class NodeStateTracker implements StatisticGatherer {
    private String lastState;

    private Map<String, LongAdder> stateCounter = LazyMap.lazyMap( new HashMap<>(), LongAdder::new );

    @Override
    public void init() {
        EventBusManager
          .createRegistrationPoint()
          .sub( NodeStateChangedEvent.class, e -> {
            if ( !e.getNewState().equals( lastState ) ) {
                stateCounter.get( e.getNewState() ).increment();
                lastState = e.getNewState();
            }
        } ).sub( WriteToStorageEvent.class, e -> {
            MetricStorage myStorage = e.storageToWriteTo()
                                       .getSubStorageCalled( "clustering" )
                                       .getSubStorageCalled( "nodestate" );
            stateCounter.keySet().forEach( nodeState -> myStorage.store( nodeState,
                                                                         stateCounter.get( nodeState ).doubleValue() ) );
        } );
    }
}
