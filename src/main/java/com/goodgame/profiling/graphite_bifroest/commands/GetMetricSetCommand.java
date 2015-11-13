package com.goodgame.profiling.graphite_bifroest.commands;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import org.apache.commons.collections4.iterators.FilterIterator;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
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
public final class GetMetricSetCommand< E extends EnvironmentWithCassandra & EnvironmentWithRetentionStrategy & EnvironmentWithMetricCache & EnvironmentWithClustering> implements Command<E> {
    public static final String COMMAND = "get-metric-set";

    private static final Logger log = LogManager.getLogger();

    private final DirectProgramStateTracker stateTracker = DirectProgramStateTracker.newTracker();

    public GetMetricSetCommand() {
        EventBusManager.createRegistrationPoint().sub(WriteToStorageEvent.class, e -> {
           stateTracker.storeIn( e.storageToWriteTo().getSubStorageCalled( "commandExecution" )
                                                            .getSubStorageCalled( "get-metric-set" )
                                                            .getSubStorageCalled( "stage-timing" ) );
        });
    }

    @Override
    public String getJSONCommand() {
        return COMMAND;
    }

    @Override
    public List<Pair<String, Boolean>> getParameters() {
        return Arrays.asList(
                new ImmutablePair<>( "name", true ),
                new ImmutablePair<>( "startTimestamp", true ),
                new ImmutablePair<>( "endTimestamp", true ) );
    }

    @Override
    public void addRequirements( Collection<String> dependencies ) {
        dependencies.add( BifroestIdentifiers.REBUILDER );
    }
    
    @Override
    public JSONObject execute( JSONObject input, E environment ){
        try {
            stateTracker.startState( "start" );
            final String name = input.getString("name");
            ThreadContext.put( "metric", name );

            JSONObject result = new JSONObject();

            BifroestClustering cluster = environment.getClustering();
            if ( cluster.doIAnswerGetMetricSets() ) {
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
            if( !metricValues.isPresent() ) {
                log.debug( "getting metrics from database" );
                stateTracker.startState( "fallback-to-db" );
                final long step = findFrequency( interval.start(), name, environment.retentions() );
                if(step <= 0) {
                    throw new IllegalStateException("Step is " + step + " - did you configure a retention for metric " 
                            + name + " or is " + interval + " in the future?");
                }
                interval = Aggregator.alignInterval( interval, step );
                Iterable<Metric> iterableMetrics = getMetrics( environment, name, interval );
                metricValues = Optional.of( Aggregator.aggregate( name, iterableMetrics, interval, step, environment.retentions() ) );
            }
            
            stateTracker.startState( "serialization" );
            JSONObject metricValuesJSON = metricValues.get().toJSON();
            metricValuesJSON.put( "answered", result.getBoolean( "answered") );
            metricValuesJSON.put( "cached-here", result.getBoolean( "cached-here") );
            return metricValuesJSON;
        } finally {
            stateTracker.stopState();
        }
    }

    private Iterable<Metric> getMetrics( E environment, String name, Interval interval ) {
        return () -> new FilterIterator<>( environment.cassandraAccessLayer().loadMetrics( name, interval ).iterator(), metric -> interval.contains( metric.timestamp() ) );
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
}
