package com.goodgame.profiling.graphite_bifroest.commands;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.goodgame.profiling.commons.systems.configuration.EnvironmentWithJSONConfiguration;
import com.goodgame.profiling.commons.systems.configuration.JSONConfigurationLoader;
import com.goodgame.profiling.commons.systems.net.jsonserver.Command;
import com.goodgame.profiling.graphite_bifroest.systems.prefixtree.EnvironmentWithPrefixTree;
import com.goodgame.profiling.graphite_bifroest.systems.prefixtree.PrefixTree;

public class GetAgeCommandTest {
    private interface TestEnvironment extends EnvironmentWithPrefixTree, EnvironmentWithJSONConfiguration {
    }

    @Mock private TestEnvironment environment;
    @Mock private JSONConfigurationLoader configLoader;

    @Before
    public void initMocks() {
        MockitoAnnotations.initMocks( this );

        PrefixTree tree = new PrefixTree( 42);
        tree.addEntry( "foo.baz", 1 );
        tree.addEntry( "abc", 2 );

        JSONArray blacklist = new JSONArray();
        blacklist.put( "foo" );
        blacklist.put( "foo.bar" );
        JSONObject config = new JSONObject().put( "bifroest", new JSONObject().put( "blacklist", blacklist ) );

        when( environment.getTree() ).thenReturn( tree );
        when( environment.getConfiguration() ).thenReturn( config );
        when( environment.getConfigurationLoader() ).thenReturn( configLoader );
    }

    @Test
    public void testGetAgeCommand() {
        Command<TestEnvironment> cmd = new GetAgeCommand<>();
        JSONObject request = new JSONObject();
        request.put( "command", "get-metric-age" );
        request.put( "metric-prefix", "" );

        JSONObject answer = cmd.execute( request, environment );
        assertTrue( answer.getBoolean( "found" ) );
        assertEquals( 2, answer.getInt( "age" ) );
    }
}
