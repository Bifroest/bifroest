package com.goodgame.profiling.graphite_bifroest.commands;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.commons.collections4.iterators.FilterIterator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.commons.model.Interval;
import com.goodgame.profiling.commons.model.Metric;
import com.goodgame.profiling.commons.statistics.DirectProgramStateTracker;
import com.goodgame.profiling.commons.statistics.WriteToStorageEvent;
import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;
import com.goodgame.profiling.commons.systems.net.jsonserver.Command;
import com.goodgame.profiling.graphite_bifroest.clustering.BifroestClustering;
import com.goodgame.profiling.graphite_bifroest.clustering.EnvironmentWithClustering;
import com.goodgame.profiling.graphite_bifroest.metric_cache.EnvironmentWithMetricCache;
import com.goodgame.profiling.graphite_bifroest.systems.BifroestIdentifiers;
import com.goodgame.profiling.graphite_bifroest.systems.cassandra.EnvironmentWithCassandra;
import com.goodgame.profiling.graphite_retentions.Aggregator;
import com.goodgame.profiling.graphite_retentions.MetricSet;
import com.goodgame.profiling.graphite_retentions.RetentionConfiguration;
import com.goodgame.profiling.graphite_retentions.RetentionLevel;
import com.goodgame.profiling.graphite_retentions.bootloader.EnvironmentWithRetentionStrategy;

@MetaInfServices
public final class GetValueCommand< E extends EnvironmentWithCassandra & EnvironmentWithRetentionStrategy & EnvironmentWithMetricCache & EnvironmentWithClustering > implements Command<E> {
    public static final String COMMAND = "get_values";

    private static final Logger log = LogManager.getLogger();

    private final DirectProgramStateTracker stateTracker = DirectProgramStateTracker.newTracker();

    public GetValueCommand() {
        EventBusManager.createRegistrationPoint().sub( WriteToStorageEvent.class, e -> {
            stateTracker.storeIn( e.storageToWriteTo().getSubStorageCalled( "commandExecution" )
                                                      .getSubStorageCalled( "get_values" )
                                                      .getSubStorageCalled( "stage-timing" ) );
        });
    }

    @Override
    public String getJSONCommand() {
        return COMMAND;
    }

    @Override
    public List<Pair<String, Boolean>> getParameters() {
        Pair<String, Boolean> paramName = new ImmutablePair<>( "name", true );
        Pair<String, Boolean> paramStart = new ImmutablePair<>( "startTimestamp", true );
        Pair<String, Boolean> paramEnd = new ImmutablePair<>( "endTimestamp", true );
        return Arrays.asList( paramName, paramStart, paramEnd );
    }

    @Override
    public JSONObject execute( JSONObject input, E environment ){
    	log.trace("entered get_value execute");
        try {
            stateTracker.startState( "responsibility-checks" );
            final String name = input.getString( "name" );
            ThreadContext.put( "metric", name );

            JSONObject result = new JSONObject();

            BifroestClustering cluster = environment.getClustering();
            if ( cluster.doIAnswerGetMetrics() ) {
                result.put( "answered", true );
            } else {
                log.info( "Not answering metric requests." );
                stateTracker.startState( "not-answering-request" );
                return result.put( "answered", false );
            }

            NodeMetadata responsibleNode = cluster.whichNodeCachesMetric( name );
            if ( responsibleNode.equals( cluster.getMyOwnMetadata() ) ) {
                result.put( "cached-here", true );
            } else {
                log.info( "Not responsible for this metric. Rejecting request." );
                stateTracker.startState( "not-responsible" );
                return result.put( "cached-here", false );
            }
            if ( cluster.doIPreloadMetric( name ) ) {
                log.info( "Still preloading this metric. Rejecting request." );
                stateTracker.startState( "preloading-metric" );
                return result.put( "preloading-metric", true );
            }

            stateTracker.startState( "retention-considerations" );

            Interval interval = getIntervalFromJSON( input );
            
            log.debug( "Performing fetch for metric " + name + " for " + interval.toString() );
            
            stateTracker.startState( "load-cache" );
            Optional<MetricSet> metricValues = environment.metricCache().getValues(name, interval);
            if( !metricValues.isPresent() ){
                log.debug("getting metrics from database");
                stateTracker.startState( "fallback-to-db" );
                final long step = findFrequency( interval.start(), name, environment.retentions() );
                interval = Aggregator.alignInterval( interval, step );
                Iterable<Metric> iterableMetrics = getMetrics( environment, name, interval );
                metricValues = Optional.of(Aggregator.aggregate(name, iterableMetrics, interval, step, environment.retentions()));
                
            }

            result.put( "time_def", makeTimespec( metricValues.get().interval(), metricValues.get().step()) );
            result.put( "values", makeValues( metricValues.get().values() ) );
            return result;
        } finally {
            stateTracker.stopState();
        }
    }
    
    private Iterable<Metric> getMetrics( E environment, String name, Interval interval ) {
        return new Iterable<Metric>() {
            @Override
            public Iterator<Metric> iterator() {
                return new FilterIterator<>( environment.cassandraAccessLayer().loadMetrics( name, interval ).iterator(), metric -> interval.contains( metric.timestamp() ) );
            }
        };
    }
    
    private static long findFrequency( long timestamp, String metric, RetentionConfiguration retentionConfiguration ){
    	Optional<RetentionLevel> optLevel = retentionConfiguration.findAccessLevelForMetric(metric);
    	long frequency = 0;
    	long now = System.currentTimeMillis() / 1000;
    	while( timestamp <= now && optLevel.isPresent() ){
    		RetentionLevel level = optLevel.get();
    		frequency = level.frequency();
    		now = now - ( now % level.blockSize() );
    		now = now + level.blockSize() - level.size();
    		optLevel = retentionConfiguration.getNextLevel(level);
    	}
    	return frequency;
    }

    private static Interval getIntervalFromJSON( JSONObject json ) {
        long start = json.getLong( "startTimestamp" );
        long end = json.getLong( "endTimestamp" );

        if ( start < 0 ) {
            throw new IllegalArgumentException( "Negative start timestamp: " + start );
        } else if ( end < 0 ) {
            throw new IllegalArgumentException( "Negative end timestamp: " + start );
        } else if ( end < start ) {
            log.warn( "start < end: " + start + " < " + end );
            throw new IllegalArgumentException( "start < end: " + start + " < " + end );
        }

        return new Interval( start, end );
    }

    private static JSONObject makeTimespec( Interval interval, long frequency ) {
        JSONObject timespec = new JSONObject();
        timespec.put( "start", interval.start() );
        timespec.put( "end", interval.end() );
        timespec.put( "step", frequency );
        return timespec;
    }

    private static JSONArray makeValues( double[] metricValues ) {
        JSONArray values = new JSONArray();
        for ( int i = 0; i < metricValues.length; i++ ) {
            if ( Double.isNaN( metricValues[i] ) ) {
                values.put( i, JSONObject.NULL );
            } else {
                values.put( i, metricValues[i] );
            }
        }

        if ( log.isTraceEnabled() ) {
            int valuesFoundTrue = 0;
            for ( int ii = 0; ii < metricValues.length; ii++ ) {
                if ( Double.isNaN( metricValues[ii] ) ) {
                    valuesFoundTrue++;
                }
            }
            log.trace( "Found " + valuesFoundTrue + " values" );
        }
        return values;
    }

    @Override
    public void addRequirements( Collection<String> dependencies ) {
        dependencies.add( BifroestIdentifiers.REBUILDER );
    }
}
