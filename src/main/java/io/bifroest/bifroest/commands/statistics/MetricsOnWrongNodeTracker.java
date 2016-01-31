package com.goodgame.profiling.graphite_bifroest.commands.statistics;

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
public class MetricsOnWrongNodeTracker implements StatisticGatherer {
    private final Map<String, LongAdder> forwardedMetrics = LazyMap.lazyMap( new HashMap<>(), LongAdder::new  );
    private final Map<String, LongAdder> droppedMetrics = LazyMap.lazyMap( new HashMap<>(), LongAdder::new  );

    @Override
    public void init() {
        EventBusManager
            .createRegistrationPoint()
            .sub(ForwardedMetricsEvent.class, e -> {
                forwardedMetrics.get( e.getSource() ).add( e.getNumberOfMetrics() );
            }).sub(DroppedMetricsEvent.class, e -> {
                droppedMetrics.get( e.getSource() ).add( e.getNumberOfMetrics() );
            }).sub(WriteToStorageEvent.class, e -> {
                MetricStorage storage = e.storageToWriteTo();
                MetricStorage subStorage = storage.getSubStorageCalled( "metrics-on-wrong-node" );

                MetricStorage forwardedMetricsSubStorage = subStorage.getSubStorageCalled( "forwarded" );
                MetricStorage droppedMetricsSubStorage = subStorage.getSubStorageCalled( "dropped" );
                
                forwardedMetrics.forEach( (k,v) -> forwardedMetricsSubStorage.store( k, v.doubleValue() ) );
                droppedMetrics.forEach( (k,v) -> droppedMetricsSubStorage.store( k, v.doubleValue() ) );
        });
    }
}
