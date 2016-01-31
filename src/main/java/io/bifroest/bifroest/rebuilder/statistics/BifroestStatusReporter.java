package io.bifroest.bifroest.rebuilder.statistics;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.statistics.WriteToStorageEvent;
import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.commons.statistics.gathering.StatisticGatherer;
import io.bifroest.commons.statistics.storage.MetricStorage;
import io.bifroest.commons.statistics.units.format.DurationFormatter;
import io.bifroest.commons.util.stopwatch.AsyncClock;
import io.bifroest.commons.util.stopwatch.Stopwatch;

@MetaInfServices
public class BifroestStatusReporter implements StatisticGatherer {
    private static final Logger log = LogManager.getLogger();

    private static final DurationFormatter formatter = new DurationFormatter();

    private AsyncClock clock = new AsyncClock();
    private Stopwatch totalTime = new Stopwatch( clock );
    private Stopwatch thisUpdateTime = new Stopwatch( clock );

    private volatile int numMetrics = 0;

    @Override
    public void init() {
        EventBusManager
          .createRegistrationPoint()
          .sub( NewMetricInsertedEvent.class, e -> numMetrics += 1 )
          .sub( RebuildStartedEvent.class, e -> {
              log.info( "Rebuild started at " + ZonedDateTime.ofInstant( e.when(), ZoneId.of( "Europe/Berlin" ) ) );
              clock.setInstant( e.when() );
              totalTime.start();
              thisUpdateTime.reset();
              thisUpdateTime.start();
          }).sub( RebuildFinishedEvent.class, e -> {
              log.info( "Rebuild finished at " + ZonedDateTime.ofInstant( e.when(), ZoneId.of( "Europe/Berlin" ) ) );
              clock.setInstant( e.when() );
              totalTime.stop();
              thisUpdateTime.stop();
              Duration rebuildTime = thisUpdateTime.duration();
              log.info( "Duration of the Tree-Rebuild: " + formatter.format( rebuildTime ) );
          }).sub( WriteToStorageEvent.class, event -> {
              event.storageToWriteTo().store( "numMetrics", numMetrics );

              MetricStorage rebuildStorage = event.storageToWriteTo().getSubStorageCalled( "Rebuild" );
              rebuildStorage.store( "totalTimeNanos", totalTime.duration().toNanos() );
              rebuildStorage.store( "thisRebuildTimeNanos", thisUpdateTime.duration().toNanos() );
          });
    }
}
