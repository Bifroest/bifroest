package io.bifroest.bifroest.clustering;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.kohsuke.MetaInfServices;

import io.bifroest.bifroest_client.metadata.PortMap;
import io.bifroest.bifroest_client.seeds.HostPortPair;
import io.bifroest.commons.boot.interfaces.Subsystem;
import io.bifroest.commons.statistics.units.parse.DurationParser;
import io.bifroest.bifroest.metric_cache.EnvironmentWithMetricCache;
import io.bifroest.bifroest.systems.BifroestIdentifiers;
import io.bifroest.commons.SystemIdentifiers;


@MetaInfServices
public class ClusterSystem<E extends EnvironmentWithMutableClustering & EnvironmentWithMetricCache> implements Subsystem<E>{
    private static final Logger log = LogManager.getLogger();

    private List<HostPortPair> seeds;
    private Duration pingFrequency;
    private PortMap internalPortmap;

    @Override
    public String getSystemIdentifier() {
        return BifroestIdentifiers.CLUSTERING;
    }

    @Override
    public Collection<String> getRequiredSystems() {
        return Arrays.asList( BifroestIdentifiers.METRIC_CACHE, SystemIdentifiers.STATISTICS );
    }

    @Override
    public void configure( JSONObject configuration ) {
        JSONObject systemConfig = configuration.getJSONObject( "clustering" );

        seeds = new ArrayList<>();
        JSONArray configSeeds = systemConfig.getJSONArray( "seeds" );
        for ( int i = 0; i < configSeeds.length(); i++ ) {
            JSONObject configSeed = configSeeds.getJSONObject( i );
            seeds.add( HostPortPair.of( configSeed.getString( "host" ),
                                        configSeed.getInt( "port" ) ) );

        }
        pingFrequency = new DurationParser().parse( systemConfig.getString( "ping-frequency" ) );
        internalPortmap = BifroestPortmap.fromConfig( configuration );
    }

    @Override
    public void boot( E environment ) throws Exception {
        BifroestClustering clustering = new BifroestClustering( seeds, internalPortmap, environment.metricCache(), pingFrequency );
        log.info( "I am {}", clustering.getMyOwnMetadata() );
        environment.setClustering( clustering );
        environment.getClustering().startClusteringSoon();
    }

    @Override
    public void shutdown( E environment ) {
        environment.getClustering().shutdown();
    }
}
