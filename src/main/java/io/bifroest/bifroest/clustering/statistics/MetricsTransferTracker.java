package io.bifroest.bifroest.clustering.statistics;

import org.kohsuke.MetaInfServices;

import io.bifroest.commons.statistics.WriteToStorageEvent;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.gathering.StatisticGatherer;

@MetaInfServices
public class MetricsTransferTracker implements StatisticGatherer {
    private long transferredMetricsReceivedCount;
    private long transferredMetricsSentCount;
    private long abandonedMetricsReceivedCount;
    private long abandonedMetricsSentCount;

    @Override
    public void init() {
        EventBusManager
           .createRegistrationPoint()
           .sub( TransferredMetricsReceivedEvent.class, e -> transferredMetricsReceivedCount += e.getCount() )
           .sub( WriteToStorageEvent.class, e -> {
              e.storageToWriteTo()
               .getSubStorageCalled( "clustering" )
               .getSubStorageCalled( "metrics-transfer" )
               .store( "transferred-metrics.received", transferredMetricsReceivedCount );
        });

        EventBusManager
           .createRegistrationPoint()
           .sub( TransferredMetricsSentEvent.class, e -> transferredMetricsSentCount += e.getCount() )
           .sub( WriteToStorageEvent.class, e -> {
              e.storageToWriteTo()
               .getSubStorageCalled( "clustering" )
               .getSubStorageCalled( "metrics-transfer" )
               .store( "transferred-metrics.sent", transferredMetricsSentCount );
        });

        EventBusManager.createRegistrationPoint()
           .sub( AbandonedMetricsReceivedEvent.class, e -> abandonedMetricsReceivedCount += e.getCount() )
           .sub( WriteToStorageEvent.class, e -> {
              e.storageToWriteTo()
               .getSubStorageCalled( "clustering" )
               .getSubStorageCalled( "metrics-transfer" )
               .store( "abandoned-metrics.received", abandonedMetricsReceivedCount );
        });

        EventBusManager.createRegistrationPoint()
           .sub( AbandonedMetricsSentEvent.class, e -> abandonedMetricsSentCount += e.getCount() )
           .sub( WriteToStorageEvent.class, e -> {
              e.storageToWriteTo()
               .getSubStorageCalled( "clustering" )
               .getSubStorageCalled( "metrics-transfer" )
               .store( "abandoned-metrics.sent", abandonedMetricsSentCount );
        });
    }
}
