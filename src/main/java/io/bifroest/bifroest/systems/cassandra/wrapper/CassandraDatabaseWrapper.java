package com.goodgame.profiling.graphite_bifroest.systems.cassandra.wrapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.goodgame.profiling.commons.model.Interval;
import com.goodgame.profiling.commons.model.Metric;
import com.goodgame.profiling.graphite_bifroest.systems.cassandra.CassandraAccessLayer;
import com.goodgame.profiling.graphite_bifroest.systems.cassandra.CassandraDatabase;
import com.goodgame.profiling.graphite_retentions.RetentionLevel;
import com.goodgame.profiling.graphite_retentions.RetentionTable;
import com.goodgame.profiling.graphite_retentions.bootloader.EnvironmentWithRetentionStrategy;

public class CassandraDatabaseWrapper< E extends EnvironmentWithRetentionStrategy > implements CassandraAccessLayer {
    private static final Logger log = LogManager.getLogger();

    private final CassandraDatabase<E> database;
    private final E environment;

    public CassandraDatabaseWrapper( CassandraDatabase<E> database, E environment ) {
        this.database = database;
        this.environment = environment;
    }

    @Override
    public Stream<Metric> loadMetrics( final String name, final Interval interval ) {

        Set<RetentionLevel> levels = new HashSet<>();
        Optional<RetentionLevel> readLevel = environment.retentions().findAccessLevelForMetric( name );
        while ( readLevel.isPresent() && !levels.contains( readLevel ) ) {
            levels.add( readLevel.get() );
            readLevel = environment.retentions().getNextLevel( readLevel.get() );

        }

        Collection<Future<Collection<Metric>>> metricsPerTable = new ArrayList<>();
        for ( RetentionTable table : database.loadTables() ) {
            if ( levels.contains( table.level() ) && table.getInterval().intersects( interval ) ) {
                metricsPerTable.add( database.loadMetrics( table, name, interval ) );
            }
        }

        Stream<Metric> result = Stream.empty();
        for ( Future<Collection<Metric>> future : metricsPerTable ) {
            try {
                result = Stream.concat( result, future.get().stream() );
            } catch ( InterruptedException | ExecutionException e ) {
                log.warn( "Exception while waiting for database-future", e );
            }
        }
        return result;
    }
}