package com.goodgame.profiling.graphite_bifroest.systems.rebuilder.statistics;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.commons.statistics.WriteToStorageEvent;
import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;
import com.goodgame.profiling.commons.statistics.gathering.StatisticGatherer;
import com.goodgame.profiling.commons.statistics.storage.MetricStorage;
import com.goodgame.profiling.commons.statistics.units.format.DurationFormatter;
import com.goodgame.profiling.commons.util.stopwatch.AsyncClock;
import com.goodgame.profiling.commons.util.stopwatch.Stopwatch;

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
