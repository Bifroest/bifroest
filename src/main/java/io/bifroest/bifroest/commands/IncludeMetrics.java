package com.goodgame.profiling.graphite_bifroest.commands;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.Factory;
import org.apache.commons.collections4.map.LazyMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.bifroest.bifroest_client.util.JSONClient;
import com.goodgame.profiling.commons.model.Metric;
import com.goodgame.profiling.commons.statistics.ProgramStateChanged;
import com.goodgame.profiling.commons.statistics.SimpleProgramStateTracker;
import com.goodgame.profiling.commons.systems.net.jsonserver.Command;
import com.goodgame.profiling.graphite_bifroest.clustering.BifroestClustering;
import com.goodgame.profiling.graphite_bifroest.clustering.EnvironmentWithClustering;
import com.goodgame.profiling.graphite_bifroest.commands.statistics.DroppedMetricsEvent;
import com.goodgame.profiling.graphite_bifroest.commands.statistics.ForwardedMetricsEvent;
import com.goodgame.profiling.graphite_bifroest.metric_cache.EnvironmentWithMetricCache;
import com.goodgame.profiling.graphite_bifroest.systems.BifroestIdentifiers;
import com.goodgame.profiling.graphite_bifroest.systems.prefixtree.EnvironmentWithPrefixTree;

@MetaInfServices
public class IncludeMetrics<E extends EnvironmentWithPrefixTree & EnvironmentWithMetricCache & EnvironmentWithClustering> implements Command<E> {
    private static final Logger log = LogManager.getLogger();

    public static final String COMMAND = "include-metrics";

    public IncludeMetrics(){
        SimpleProgramStateTracker.forContext("bifroest-bifroest-includeMetrics").storingIn("commandExecution.include-metrics.stage-timing").build();
    }
    
    @Override
    public String getJSONCommand() {
        return COMMAND;
    }

    @Override
    public List<Pair<String, Boolean>> getParameters() {
        return Collections.<Pair<String, Boolean>> singletonList( new ImmutablePair<>( "metrics", true ) );
    }

    @Override
    public JSONObject execute( JSONObject input, E environment ) {
        BifroestClustering cluster = environment.getClustering();
        JSONArray metricsNotCachedHere = new JSONArray();
        JSONObject result = new JSONObject();
        result.put( "cached-here", true );

        ProgramStateChanged.fireContextChangeToState("bifroest-bifroest-includeMetrics", "StartIncludeMetrics");
        JSONArray metrics = input.getJSONArray( "metrics" );
        log.trace( "Include {} metrics", metrics.length() );

        for ( int i = 0; i < metrics.length(); i++ ) {
            ProgramStateChanged.fireContextChangeToState("bifroest-bifroest-includeMetrics", "JSONGetMetricConfig");
            JSONObject metric = metrics.getJSONObject( i );
            String metricName = metric.getString( "name" );
            log.trace( "Will include metric '{}'", metricName );

            NodeMetadata responsibleNode = cluster.whichNodeCachesMetric( metricName );
            if ( responsibleNode.equals( cluster.getMyOwnMetadata() ) ) {
                log.trace( "Node is responsible for metric '{}'", metricName );
                long timestamp = metric.getLong( "timestamp" );
                double value = metric.getDouble( "value" );
                ProgramStateChanged.fireContextChangeToState("bifroest-bifroest-includeMetrics", "TreeAddEntry");
                log.trace( "Adding metric '{}' with value {} and timestamp {} to tree", metricName, value, timestamp );
                environment.getTree().addEntry( metricName, timestamp );
                ProgramStateChanged.fireContextChangeToState("bifroest-bifroest-includeMetrics", "CreateMetricObject");
                log.trace( "Adding metric to metric cache" );
                environment.metricCache().put( new Metric( metricName, timestamp, value ) );
            } else {
                log.trace( "Node is NOT responsible for metric '{}'", metricName );
                ProgramStateChanged.fireContextChangeToState("bifroest-bifroest-includeMetrics", "RejectMetric");
                metricsNotCachedHere.put( metric );
            }
        }

        ProgramStateChanged.fireContextStopped("bifroest-bifroest-includeMetrics");
        if ( metricsNotCachedHere.length() > 0 ) {
            result.put( "cached-here", false );
            result.put( "metrics-not-cached-here", metricsNotCachedHere );
        }
        log.trace( "{} of {} metrics are cached by this node", metrics.length() - metricsNotCachedHere.length(), metrics.length() );
        return result;
    }

    @Override
    public void addRequirements( Collection<String> dependencies ) {
        dependencies.add( BifroestIdentifiers.REBUILDER );
    }
}
