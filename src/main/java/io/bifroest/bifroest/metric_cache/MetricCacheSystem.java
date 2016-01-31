package io.bifroest.bifroest.metric_cache;

import java.util.Arrays;
import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.json.JSONArray;
import org.kohsuke.MetaInfServices;

import io.bifroest.commons.boot.interfaces.Subsystem;
import io.bifroest.bifroest.systems.BifroestEnvironment;
import io.bifroest.bifroest.systems.BifroestIdentifiers;
import io.bifroest.bifroest.systems.cassandra.EnvironmentWithCassandra;
import io.bifroest.commons.SystemIdentifiers;
import io.bifroest.commons.configuration.ConfigurationObserver;
import io.bifroest.commons.configuration.EnvironmentWithJSONConfiguration;
import io.bifroest.commons.configuration.InvalidConfigurationException;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.bootloader.EnvironmentWithRetentionStrategy;

@MetaInfServices
public class MetricCacheSystem<E extends EnvironmentWithRetentionStrategy & EnvironmentWithJSONConfiguration & EnvironmentWithCassandra & EnvironmentWithMutableMetricCache>
        implements Subsystem<BifroestEnvironment> {
    private static final Logger log = LogManager.getLogger();

    @Override
    public String getSystemIdentifier() {
        return BifroestIdentifiers.METRIC_CACHE;
    }

    @Override
    public Collection<String> getRequiredSystems() {
        return Arrays.asList( SystemIdentifiers.RETENTION, BifroestIdentifiers.CASSANDRA, SystemIdentifiers.STATISTICS );
    }

    @Override
    public void boot( BifroestEnvironment environment ) throws Exception {
        CachingConfiguration cachingConfig = createCaching( environment.getConfiguration(), environment.retentions() );
        environment.setCachingConfiguration( cachingConfig );
        environment.setMetricCache( new MetricCache( environment ) );
        log.debug( "environment contains caching: " + environment.getConfiguration().toString() );
        log.debug( "environment contains retention: " + environment.retentions().toString() );
        environment.getConfigurationLoader().subscribe( new ConfigurationObserver() {

            @Override
            public void handleNewConfig( JSONObject config ) {
                try {
                    CachingConfiguration cachingConfig = createCaching( config, environment.retentions() );
                    environment.setCachingConfiguration( cachingConfig );
                    if ( environment.metricCache() == null ) {
                        log.debug( "no caching configuration existing. creating new configuration" );
                        environment.setMetricCache( new MetricCache( environment ) );
                    }
                    else {
                        log.debug( "updating caching configuration" );
                        environment.metricCache().updateConfiguration( environment.retentions(), cachingConfig );
                        log.debug( ">reload callback< environment contains caching: " + environment.getConfiguration().toString() );
                        log.debug( ">reload callback< environment contains retention: " + environment.retentions().toString() );
                    }
                } catch( InvalidConfigurationException e ) {
                    log.error( e );
                }
            }
        } );

    }

    @Override
    public void shutdown( BifroestEnvironment environment ) {
        environment.metricCache().writeMostRequestedMetricsToFiles();
        environment.metricCache().shutdown();
        environment.setMetricCache( null );
    }
    
    private static CachingConfiguration createCaching(JSONObject config, RetentionConfiguration retentionConfig) throws InvalidConfigurationException{
        log.entry(config);

        JSONObject cache = config.getJSONObject("cache");
        double QoS = cache.getDouble("quality-of-service");
        double percentageToSave = cache.getDouble("percentage-of-metrics-to-save");
        double percentageToTransfer = cache.getDouble("percentage-of-metrics-to-transfer");
        String metricStorage = cache.getString("metric-storage");
        int databaseQueryThreads = cache.getInt("number-of-database-threads");
        int metricCacheThreads = cache.getInt("number-of-metriccache-threads");
        
        CachingConfiguration caching = new CachingConfiguration(retentionConfig, metricStorage, QoS, percentageToSave, percentageToTransfer, databaseQueryThreads, metricCacheThreads);
        
        JSONObject levels = cache.getJSONObject("levels");
        JSONArray names = levels.names();
        
        for(int i = 0; i<names.length(); i++){
            JSONObject level = levels.getJSONObject(names.getString(i));
            String name = names.getString(i).toLowerCase();
            int totalCacheSize = 0;
            int visibleCacheSize = 0;
            if(level.has("visibleCacheSize")){
                visibleCacheSize = level.getInt("visibleCacheSize");
            }
            if(level.has("totalCacheSize")){
                totalCacheSize = level.getInt("totalCacheSize");
            }
            int cacheLineWidth = level.getInt("cacheLineWidth");
            
            caching.addLevel( new CachingLevel( name, visibleCacheSize, totalCacheSize, cacheLineWidth ) );
        }
        caching.correctCacheSizes();
        return log.exit(caching);
    } 

    @Override
    public void configure( JSONObject configuration ) {
        // empty
    }
}
