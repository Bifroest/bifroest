package io.bifroest.bifroest.metric_cache.statistics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.collections4.map.LazyMap;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.statistics.WriteToStorageEvent;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.gathering.StatisticGatherer;
import io.bifroest.commons.statistics.storage.MetricStorage;

@MetaInfServices
public class RaceConditionCounter implements StatisticGatherer {
    private Map<String, LongAdder> raceConditions = LazyMap.lazyMap( new HashMap<>(), LongAdder::new );

    @Override
    public void init() {
        EventBusManager
            .createRegistrationPoint()
            .sub( RaceConditionTriggeredEvent.class, e -> raceConditions.get( e.cacheName() ).increment() )
            .sub( WriteToStorageEvent.class, e -> {
                MetricStorage storage = e.storageToWriteTo().getSubStorageCalled( "Caches" );
                for( String cacheName : raceConditions.keySet() ) {
                    storage.getSubStorageCalled( cacheName ).store( "race-conditions", raceConditions.get( cacheName ).longValue() );
                }
            });
    }
}
