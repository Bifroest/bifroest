package com.goodgame.profiling.graphite_bifroest.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.stream.Stream;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.commons.model.Interval;
import com.goodgame.profiling.commons.model.Metric;
import com.goodgame.profiling.commons.statistics.aggregation.LastAggregation;
import com.goodgame.profiling.commons.statistics.aggregation.ValueAggregation;
import com.goodgame.profiling.graphite_bifroest.clustering.BifroestClustering;
import com.goodgame.profiling.graphite_bifroest.clustering.EnvironmentWithClustering;
import com.goodgame.profiling.graphite_bifroest.clustering.state.JSONObjectMatcher;
import com.goodgame.profiling.graphite_bifroest.metric_cache.CachingConfiguration;
import com.goodgame.profiling.graphite_bifroest.metric_cache.CachingLevel;
import com.goodgame.profiling.graphite_bifroest.metric_cache.EnvironmentWithMetricCache;
import com.goodgame.profiling.graphite_bifroest.metric_cache.MetricCache;
import com.goodgame.profiling.graphite_bifroest.systems.cassandra.CassandraAccessLayer;
import com.goodgame.profiling.graphite_bifroest.systems.cassandra.EnvironmentWithCassandra;
import com.goodgame.profiling.graphite_retentions.Aggregator;
import com.goodgame.profiling.graphite_retentions.RetentionConfiguration;
import com.goodgame.profiling.graphite_retentions.RetentionLevel;
import com.goodgame.profiling.graphite_retentions.bootloader.EnvironmentWithRetentionStrategy;

public final class GetValueCommandTest {

    private static final String NAME = "metric name";

    private interface TestEnvironment extends EnvironmentWithCassandra, EnvironmentWithRetentionStrategy, EnvironmentWithMetricCache, EnvironmentWithClustering {
    }

    @Mock
    private TestEnvironment environment;

    @Mock
    private CassandraAccessLayer database;

    @Mock
    private RetentionConfiguration retentions;
    
    @Mock
    private CachingConfiguration caching;

    @Mock
    private MetricCache cache;

    @Mock
    private BifroestClustering cluster;

    private NodeMetadata myOwnMetadata;

    @Before
    public void createMocks() {
        MockitoAnnotations.initMocks( this );

        RetentionLevel levelA = new RetentionLevel( "lvlA", 10, 5, 100, "lvlB" );
        RetentionLevel levelB = new RetentionLevel( "lvlB", 100, 1, 1000, null );
        
        CachingLevel clevelA = new CachingLevel( "lvlA", 5, 7, 500 );
        CachingLevel clevelB = new CachingLevel( "lvlB", 5, 7, 1000 );

        myOwnMetadata = NodeMetadata.forTest();

        when( environment.cassandraAccessLayer() ).thenReturn( database );
        when( environment.retentions() ).thenReturn( retentions );
        when( environment.cachingConfiguration() ).thenReturn( caching );
        when( environment.metricCache() ).thenReturn( cache );
        when( environment.getClustering() ).thenReturn( cluster );
        
        when( caching.findLevelForLevelName(eq("lvlA")) ).thenReturn(Optional.of(clevelA));
        when( caching.findLevelForLevelName(eq("lvlB")) ).thenReturn(Optional.of(clevelB));

        when( cache.getValues( any(), any() ) ).thenReturn( Optional.empty() );

        when( cluster.doIAnswerGetMetrics() ).thenReturn( true );
        when( cluster.whichNodeCachesMetric( any() ) ).thenReturn( myOwnMetadata );
        when( cluster.getMyOwnMetadata() ).thenReturn( myOwnMetadata );

        when( retentions.findAccessLevelForMetric( anyString() ) ).thenReturn( Optional.of(levelA));
        when( retentions.getNextLevel(levelA)).thenReturn(Optional.of(levelB));
        when( retentions.findFunctionForMetric( anyString() ) ).thenAnswer( new Answer<ValueAggregation>() {

            @Override
            public ValueAggregation answer( InvocationOnMock invocation ) throws Throwable {
                return new LastAggregation();
            }

        } );
    }

    @Test
    public void testCommandFromJSON() {
        assertEquals( "get_values", new GetValueCommand<TestEnvironment>().getJSONCommand() );
    }

    private JSONObject makeRequest( long start, long end ) {
        JSONObject input = new JSONObject();
        input.put( "command", "get_values" );
        input.put( "name", NAME );
        input.put( "startTimestamp", start );
        input.put( "endTimestamp", end );
        return input;
    }

    private JSONObject makeResult( long start, long end, long step, double... values ) {
        JSONObject result = new JSONObject();
        JSONObject timeDefinition = new JSONObject();
        timeDefinition.put( "start", start );
        timeDefinition.put( "end", end );
        timeDefinition.put( "step", step );
        result.put( "time_def", timeDefinition );

        JSONArray valueArray = new JSONArray();
        for ( int i = 0; i < values.length; i++ ) {
            valueArray.put( values[i] );
        }
        result.put( "values", valueArray );
        result.put( "answered", true );
        result.put( "cached-here", true );

        return result;
    }

    @Test( expected = IllegalArgumentException.class )
    public void testNegativeStartTimestamp() {
        JSONObject input = makeRequest( -5000, 6000 );
        new GetValueCommand<TestEnvironment>().execute( input, environment );
    }

    @Test( expected = IllegalArgumentException.class )
    public void testNegativeEndTimestamp() {
        JSONObject input = makeRequest( 5000, -6000 );
        new GetValueCommand<TestEnvironment>().execute( input, environment );
    }

    @Test( expected = IllegalArgumentException.class )
    public void testUnorderedStartEndTimestamp() {
        JSONObject input = makeRequest( 8000, 6000 );
        new GetValueCommand<TestEnvironment>().execute( input, environment );
    }

    @Test
    public void testFirstLevel() {
        long now = System.currentTimeMillis() / 1000;
        now = Aggregator.alignTo( now, 10 );
        Interval interval = new Interval( now - 350, now - 300 );

        JSONObject input = makeRequest( interval.start(), interval.end() );
        JSONObject expectedOutput = makeResult( interval.start(), interval.end(), 10, 1, 2, 3, 4, 5 );

        Stream<Metric> metrics = Stream.of(
        		new Metric( NAME, now - 310, 5 ),  
        		new Metric( NAME, now - 350, 1 ),
        		new Metric( NAME, now - 320, 4 ),
        		new Metric( NAME, now - 340, 2 ),
        		new Metric( NAME, now - 330, 3 ));
 
        when( database.loadMetrics( eq( NAME ), any( Interval.class ) ) ).thenReturn( metrics );

        JSONObject output = new GetValueCommand<TestEnvironment>().execute( input, environment );
        JSONObjectMatcher outputMatcher = new JSONObjectMatcher( output );

        assertTrue( outputMatcher.matches( expectedOutput ) );
        verify( database ).loadMetrics( NAME, interval );
    }

}
