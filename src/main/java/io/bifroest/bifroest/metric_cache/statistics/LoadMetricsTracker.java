package io.bifroest.bifroest.metric_cache.statistics;

import org.kohsuke.MetaInfServices;

import io.bifroest.commons.statistics.WriteToStorageEvent;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.gathering.StatisticGatherer;
import io.bifroest.commons.statistics.storage.MetricStorage;

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
