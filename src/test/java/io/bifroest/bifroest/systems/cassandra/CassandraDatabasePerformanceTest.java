package com.goodgame.profiling.graphite_bifroest.systems.cassandra;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.goodgame.profiling.commons.model.Interval;
import com.goodgame.profiling.commons.model.Metric;
import com.goodgame.profiling.commons.statistics.aggregation.ValueAggregation;
import com.goodgame.profiling.graphite_bifroest.systems.cassandra.CassandraDatabase;
import com.goodgame.profiling.graphite_bifroest.systems.cassandra.wrapper.CassandraDatabaseWrapper;
import com.goodgame.profiling.graphite_retentions.RetentionConfiguration;
import com.goodgame.profiling.graphite_retentions.RetentionLevel;
import com.goodgame.profiling.graphite_retentions.RetentionTable;
import com.goodgame.profiling.graphite_retentions.bootloader.EnvironmentWithRetentionStrategy;

public final class CassandraDatabasePerformanceTest {
    private static final String cassandraSeed = "cassandra01.bifroest.nl.ggs-net.com";
    private static final String keyspace = "graphite";

    public static void main( String[] args ) throws Exception {
        MyEnvironment env = new MyEnvironment();

        CassandraDatabase< MyEnvironment > db = new CassandraDatabase< >( null, null, keyspace, new String[] { cassandraSeed }, env );
        CassandraDatabaseWrapper< MyEnvironment > wrapper = new CassandraDatabaseWrapper< >( db, env );

        db.open();
        try ( Timing t = new Timing() ) {
            Collection< Metric > metrics = wrapper
                    .loadMetrics( "servers.com.ggs-net.nl.bifroest.bigweb01.Bifroest.Aggregator.EventBus.Handler-0.EventsComsumed.ProgramStateChanged",
                            new Interval( 0, 1446023515 ) )
                    .collect( Collectors.toList() );
            System.out.println( "Collected " + metrics.size() + " metrics." );
        } finally {
            db.close();
        }
    }

    private static final class Timing implements AutoCloseable {
        private final Clock clock = Clock.systemUTC();
        private final Instant start = clock.instant();

        @Override
        public void close() {
            Instant end = clock.instant();
            Duration d = Duration.between( start, end );
            System.out.println( "took " + d );
        }
    }

    private static final class MyRetentionConfiguration implements RetentionConfiguration {
        private final Map< String, RetentionLevel > levels;

        public MyRetentionConfiguration() {
            this.levels = new HashMap< >();
            this.levels.put( "tenseconds", new RetentionLevel( "tenseconds", 10, 96, 3600, "5minutes" ) );
            this.levels.put( "tenseven", new RetentionLevel( "tenseven", 10, 168, 3600, "1minute" ) );
            this.levels.put( "1minute", new RetentionLevel( "1minute", 60, 56, 21600, "hourly" ) );
            this.levels.put( "5minutes", new RetentionLevel( "5minutes", 600, 14, 86400, "hourly" ) );
            this.levels.put( "hourly", new RetentionLevel( "hourly", 3600, 18, 864000, "daily" ) );
            this.levels.put( "daily", new RetentionLevel( "daily", 86400, 8, 8640000, null ) );
        }

        @Override
        public Optional< RetentionLevel > getLevelForName( String levelname ) {
            return Optional.ofNullable( levels.get( levelname ) );
        }

        @Override
        public Optional< RetentionLevel > findAccessLevelForMetric( String name ) {
            return Optional.of( levels.get( "tenseconds" ) );
        }

        @Override
        public Optional< RetentionLevel > getNextLevel( RetentionLevel level ) {
            return Optional.ofNullable( levels.get( level.next() ) );
        }

        @Override
        public ValueAggregation findFunctionForMetric( String name ) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Optional< RetentionTable > findAccessTableForMetric( String name, long timestamp ) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Collection< RetentionLevel > getAllLevels() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public List< RetentionLevel > getAllAccessLevels() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public List< RetentionLevel > getTopologicalSort() {
            // TODO Auto-generated method stub
            return null;
        }

    }

    private static final class MyEnvironment implements EnvironmentWithRetentionStrategy {
        private final MyRetentionConfiguration retentions = new MyRetentionConfiguration();

        @Override
        public RetentionConfiguration retentions() {
            return retentions;
        }
    }
}
