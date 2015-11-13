package com.goodgame.profiling.graphite_bifroest.systems.cassandra;

import java.util.Arrays;
import java.util.Collection;

import org.json.JSONObject;
import org.kohsuke.MetaInfServices;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.goodgame.profiling.commons.boot.interfaces.Subsystem;
import com.goodgame.profiling.commons.systems.SystemIdentifiers;
import com.goodgame.profiling.commons.systems.configuration.EnvironmentWithJSONConfiguration;
import com.goodgame.profiling.commons.util.json.JSONUtils;
import com.goodgame.profiling.graphite_bifroest.systems.BifroestIdentifiers;
import com.goodgame.profiling.graphite_bifroest.systems.cassandra.wrapper.CassandraDatabaseWrapper;
import com.goodgame.profiling.graphite_retentions.bootloader.EnvironmentWithRetentionStrategy;

@MetaInfServices
public class CassandraSystem< E extends EnvironmentWithJSONConfiguration & EnvironmentWithMutableCassandra & EnvironmentWithRetentionStrategy > implements
        Subsystem<E> {
    private static final Logger log = LogManager.getLogger();
    private CassandraDatabase<E> database;

    @Override
    public String getSystemIdentifier() {
        return BifroestIdentifiers.CASSANDRA;
    }

    @Override
    public Collection<String> getRequiredSystems() {
        return Arrays.asList( SystemIdentifiers.RETENTION );
    }

    @Override
    public void boot( E environment ) throws Exception {
        JSONObject config = environment.getConfiguration().getJSONObject( "cassandra" );
        if ( config.optBoolean( "in-memory", false ) ) {
            log.warn( "Bifroest is running in memory only" );
            environment.setCassandraAccessLayer( new MemoryOnlyAccessLayer<E>(environment) );
        } else {
            log.info( "Establishing database connection" );
            String username = JSONUtils.getWithDefault( config, "username", (String) null );
            String password = JSONUtils.getWithDefault( config, "password", (String) null );
            String keyspace = config.getString( "keyspace" );
            String[] seeds = JSONUtils.getStringArray( "seeds", config );
            database = new CassandraDatabase<E>( username, password, keyspace, seeds, environment );
            database.open();

            environment.setCassandraAccessLayer( new CassandraDatabaseWrapper<E>( database, environment ) );
        }
    }

    @Override
    public void shutdown( E environment ) {
        // in-memory doesn't set this
        if ( database != null ) {
            database.close();
        }
    }

    @Override
    public void configure( JSONObject config ) {
        // empty

    }
}
