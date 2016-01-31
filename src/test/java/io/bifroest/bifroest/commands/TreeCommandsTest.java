package com.goodgame.profiling.graphite_bifroest.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.goodgame.profiling.commons.systems.configuration.EnvironmentWithJSONConfiguration;
import com.goodgame.profiling.commons.systems.configuration.JSONConfigurationLoader;
import com.goodgame.profiling.commons.systems.net.jsonserver.Command;
import com.goodgame.profiling.commons.util.json.JSONUtils;
import com.goodgame.profiling.graphite_bifroest.systems.prefixtree.EnvironmentWithPrefixTree;
import com.goodgame.profiling.graphite_bifroest.systems.prefixtree.PrefixTree;

public class TreeCommandsTest {
    private interface TestEnvironment extends EnvironmentWithPrefixTree, EnvironmentWithJSONConfiguration {
    }

    @Mock
    private TestEnvironment environment;

    @Mock
    private JSONConfigurationLoader configLoader;

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks( this );

        PrefixTree tree = new PrefixTree( 42 );
        tree.addEntry( "foo.bar.qux", 2 );
        tree.addEntry( "foo.bar.quux", 3 );
        tree.addEntry( "foo.baz", 4 );

        JSONObject config = new JSONObject().put( "bifroest", new JSONObject().put( "blacklist", new JSONArray() ) );

        when( environment.getTree() ).thenReturn( tree );
        when( environment.getConfiguration() ).thenReturn( config );
        when( environment.getConfigurationLoader() ).thenReturn( configLoader );
    }

    @Test
    public void testGetAllNodes() {
        Command<TestEnvironment> cmd = new GetAllNodesCommand<>();
        JSONObject request = new JSONObject();
        request.put( "command", "get-all-nodes" );

        JSONObject answer = cmd.execute( request, environment );
        List<String> nodes = Arrays.asList( JSONUtils.getStringArray( "result", answer ) );

        assertFalse( nodes.contains( "foo.bar" ) );
        assertTrue( nodes.contains( "foo.bar.qux" ) );
        assertTrue( nodes.contains( "foo.bar.quux" ) );
        assertTrue( nodes.contains( "foo.baz" ) );
    }

    @Test
    public void testGetSubMetrics() {
        Command<TestEnvironment> cmd = new GetSubMetricsCommand<>();
        JSONObject request = new JSONObject();
        request.put( "command", "get-sub-metrics" );
        request.put( "query", "foo.*.{qux,quux}" );

        JSONObject answer = cmd.execute( request, environment );
        JSONArray array = answer.getJSONArray( "results" );
        List<String> nodes = new ArrayList<>();
        for ( int i = 0; i < array.length(); i++ ) {
            nodes.add( array.getJSONObject( i ).getString( "path" ) );
        }

        assertFalse( nodes.contains( "foo.bar" ) );
        assertTrue( nodes.contains( "foo.bar.qux" ) );
        assertTrue( nodes.contains( "foo.bar.quux" ) );
        assertFalse( nodes.contains( "foo.baz" ) );
    }

    @Test
    public void testGetAgeCommand() {
        Command<TestEnvironment> cmd = new GetAgeCommand<>();
        JSONObject request = new JSONObject();
        request.put( "command", "get-metric-age" );
        request.put( "metric-prefix", "foo.bar" );

        JSONObject answer = cmd.execute( request, environment );
        assertTrue( answer.getBoolean( "found" ) );
        assertEquals( 2, answer.getInt( "age" ) );
    }

}
