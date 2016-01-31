package com.goodgame.profiling.graphite_bifroest.metric_cache.statistics;

import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.commons.statistics.WriteToStorageEvent;
import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;
import com.goodgame.profiling.commons.statistics.gathering.StatisticGatherer;
import com.goodgame.profiling.commons.statistics.storage.MetricStorage;

@MetaInfServices
public final class LoadMetricsTracker implements StatisticGatherer {
    long metricPreloads = 0;

    @Override
    public void init() {
        EventBusManager
            .createRegistrationPoint()
            .sub( LoadMetricEvent.class, e -> metricPreloads++ )
            .sub( WriteToStorageEvent.class, e -> {
                MetricStorage myStorage = e.storageToWriteTo().getSubStorageCalled( "metric-cache" );
                myStorage.store( "metrics-loaded", metricPreloads );
            });
    }
}
