package io.bifroest.bifroest.metric_cache;

import static java.lang.Math.max;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.bifroest.commons.model.Interval;
import io.bifroest.commons.model.Metric;
import io.bifroest.commons.statistics.ProgramStateChanged;
import io.bifroest.commons.statistics.jmx.MBeanManager;
import io.bifroest.bifroest.metric_cache.eviction_strategy.NaiveLRUStrategy;
import io.bifroest.bifroest.metric_cache.statistics.LoadMetricEvent;
import io.bifroest.bifroest.BifroestEnvironment;
import io.bifroest.commons.configuration.InvalidConfigurationException;
import io.bifroest.retentions.MetricSet;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.RetentionLevel;

public class MetricCache implements MetricCacheMBean{
    private static final Logger log = LogManager.getLogger();
    
    private final String metricStorage;
    private final Map<String, LevelCache> caches;

    private final Map<String, EvictionStrategy> evictionStrategyMap;
    private final BifroestEnvironment environment;
    private final CachingConfiguration cachingConfig;

    private final ThreadPoolExecutor loadMetricsThreadPool;
    private final ThreadPoolExecutor databaseQueryPool;
    
    public MetricCache( BifroestEnvironment environment ) {
        this.caches = new HashMap<>();
        this.evictionStrategyMap = new HashMap<>();
        this.environment = environment;
        this.metricStorage = environment.cachingConfiguration().getMetricStorage();
        this.cachingConfig = environment.cachingConfiguration();
                
        ThreadFactory threads = new BasicThreadFactory.Builder().namingPattern( "loadMetricsThread" ).build();
        loadMetricsThreadPool = new ThreadPoolExecutor(
                environment.cachingConfiguration().getMetricCacheThreads(), environment.cachingConfiguration().getMetricCacheThreads(), // thread count is set to the real initial value on the first run()
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                threads
        );

        ThreadFactory databaseThreads = new BasicThreadFactory.Builder().namingPattern( "databaseQueryThread" ).build();
        databaseQueryPool = new ThreadPoolExecutor(
                environment.cachingConfiguration().getDatabaseThreads(), environment.cachingConfiguration().getDatabaseThreads(), // thread count is set to the real initial value on the first run()
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(),
                databaseThreads
        );

        for ( RetentionLevel retentionLevel : environment.retentions().getAllLevels() ) {
            Optional<CachingLevel> cachingLevel = environment.cachingConfiguration().findLevelForLevelName( retentionLevel.name() );
            if ( cachingLevel.isPresent() ) {
                caches.put( retentionLevel.name(), new LevelCache( environment, cachingLevel.get() ) );
                log.info( "Created cache for {}", retentionLevel.name() );
            }
            else {
                log.warn( "RetentionLevel with unconfigured Cache {}", retentionLevel.name() );
            }
        }

        for ( RetentionLevel level : environment.retentions().getAllAccessLevels() ) {
            Optional<CachingLevel> cachingLevel = environment.cachingConfiguration().findLevelForLevelName( level.name() );
            if ( cachingLevel.isPresent() ) {
                int evictionStrategySize = cachingLevel.get().visibleCacheSize();
                evictionStrategyMap.put( level.name(), new NaiveLRUStrategy( level.name(), metricStorage, findAllFollowingLevelCaches( level ), evictionStrategySize ) );
                log.trace( "EvictionStrategyMap.put({})", level.name() );
            }
        }        

        MBeanManager.registerStandardMBean( this,
                MetricCache.class.getPackage().getName()  + ":type=" + MetricCache.class.getSimpleName(),
                MetricCacheMBean.class );
    }
    
    public void loadSavedMetrics() {
        List<RetentionLevel> accessLevels = environment.retentions().getAllAccessLevels();
        int numberOfAccessLevels = accessLevels.size();
        log.info("trying to load Metrics for " + numberOfAccessLevels + " accessLevels");
        List<Future<?>> futures = new ArrayList<>();
        try {
            for( RetentionLevel rlevel : accessLevels ){
                futures.addAll( loadSavedMetricsToCaches( rlevel.name() ) );
            }
        } catch ( Exception e ) {
            loadMetricsThreadPool.shutdownNow();
            try {
                loadMetricsThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException e1) {
                log.error( "Waiting for loadMetrics thread pool shutdown was interrupted", e1 );
            }
            for ( LevelCache cache : caches.values() ) {
                cache.shutdown();
            }
            throw e;
        }
        log.info( "Submitted a total of {} futures to pre-load metrics", futures.size() );
        int logEvery = Math.max( 1, futures.size() / 10 );
        int futuresDone = 0;
        for ( Future<?> f : futures ) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                log.warn( "Exception while waiting for load metrics threads", e );
            }
            futuresDone++;
            if ( futuresDone % logEvery == 0 ) {
                log.info( "Preloaded {} of {} metrics", futuresDone, futures.size() );
            }
        }
        log.info("all metrics preloaded!");
    }
    
    private List<Future<?>> loadSavedMetricsToCaches( String accessLevelName ){
        String storage = metricStorage + "/" + accessLevelName;
        Path storageFile = Paths.get( storage );
        if ( !Files.exists(storageFile) ) {
            log.info( "Cannot pre-load cache {} - file {} doesn't exist", accessLevelName, storage);
            return Collections.emptyList();
        }
        
        try ( BufferedReader fromStorageFile = new BufferedReader( new InputStreamReader( new GZIPInputStream( Files.newInputStream( storageFile ) ), Charset.forName("UTF-8") ) ) ){
            boolean firstLine = true;
            String metricName;
            List<Future<?>> futures = new ArrayList<>();
            while( ( metricName = fromStorageFile.readLine() ) != null ){
                if( firstLine ){
                    firstLine = false;
                }
                else {
                    futures.add( createNewCacheLines( metricName ) );
                }
            }
            return futures;
        } catch (IOException e) {
            log.info("could not find file for accessLevel " + accessLevelName, e);
            return Collections.emptyList();
        }
    }
    
    public void writeMostRequestedMetricsToFiles(){
        double percentage = environment.cachingConfiguration().getPercentageOfMetricsToSave();
        for( Entry<String, EvictionStrategy> entry : evictionStrategyMap.entrySet() ){
            entry.getValue().writeMostRequestedMetricsToFile(percentage);
        }
    }

    public List<String> getMostRequestedMetrics() {
        double percentage = environment.cachingConfiguration().getPercentageOfMetricsToTransfer();
        List<List<String>> mostRequestedMetrics = new ArrayList<>();
        List<String> result = new ArrayList<>();
        int maxListSize = 0;

        for( Entry<String, EvictionStrategy> entry : evictionStrategyMap.entrySet() ) {
            List<String> mostRequestedMetricsForOnelevelCache = entry.getValue().getMostRequestedMetrics( percentage );
            maxListSize = max( maxListSize, mostRequestedMetricsForOnelevelCache.size() );
            mostRequestedMetrics.add( mostRequestedMetricsForOnelevelCache );
        }

        for( int i = 0; i < maxListSize; i++ ) {
            for ( List<String> mostRequestedMetricsForOnelevelCache : mostRequestedMetrics ) {
                if( mostRequestedMetricsForOnelevelCache.size() > i ) {
                    result.add( mostRequestedMetricsForOnelevelCache.get( i ) );
                }
            }
        }
        return result;
    }

    public void updateConfiguration( RetentionConfiguration newRetentionConfig, CachingConfiguration newCachingConfig ) throws InvalidConfigurationException {
        for ( Iterator<String> nameIter = caches.keySet().iterator(); nameIter.hasNext(); ) {
            String name = nameIter.next();
            if ( !newCachingConfig.findLevelForLevelName( name ).isPresent() ) {
                caches.get( name ).shutdown();
                nameIter.remove();
                if ( evictionStrategyMap.containsKey( name ) ) {
                    evictionStrategyMap.remove( name );
                }
                log.debug( "Dropped cache for {}", name );
            }
        }
        checkAllLevelCaches();
        updateEvictionStrategies();

        log.debug("finished all updateConfiguration Acitivies!");
    }
    
    public void updateEvictionStrategies(){
        Iterator<String> iterator = evictionStrategyMap.keySet().iterator();
        while ( iterator.hasNext() ) {
            String levelname = iterator.next();
            Optional<RetentionLevel> level = environment.retentions().getLevelForName( levelname );
            if ( !level.isPresent() || !levelIsAccessLevel( level.get() ) ) {
                iterator.remove();
            }
        }
        Collection<RetentionLevel> accessLevels = environment.retentions().getAllAccessLevels();
        for( RetentionLevel accessLevel : accessLevels ){
            if( caches.containsKey(accessLevel.name()) && !evictionStrategyMap.containsKey(accessLevel.name()) ){
                Optional<CachingLevel> cachingLevel = environment.cachingConfiguration().findLevelForLevelName(accessLevel.name());
                if( cachingLevel.isPresent() ){
                    evictionStrategyMap.put(accessLevel.name(), new NaiveLRUStrategy( accessLevel.name(), metricStorage, findAllFollowingLevelCaches(accessLevel) ,cachingLevel.get().visibleCacheSize()));
                }
            }
        }
    }
    
    public void checkAllLevelCaches() throws InvalidConfigurationException{
        Collection<RetentionLevel> allLevels = environment.retentions().getAllLevels();
        List<RetentionLevel> list = new ArrayList<>();
        for(RetentionLevel level : allLevels) {            
            if( level.next() == null ){
                list.add(level);
            }
        }    
        log.trace("start updating config with top-levels: " + list);
        for( RetentionLevel level : list ){
            getLevelsAbove(level);
        }
    }
    
    private void getLevelsAbove( RetentionLevel level ) throws InvalidConfigurationException{
        Collection<RetentionLevel> list = environment.retentions().getAllLevels();
        List<RetentionLevel> pointingToCurrentLevel = new LinkedList<>();
        for( RetentionLevel rlevel : list ){
            if( rlevel.next() != null ){
                if( rlevel.next().equals(level.name()) ){
                    pointingToCurrentLevel.add( rlevel );
                }
            }
        }
        if( !pointingToCurrentLevel.isEmpty() ){
            for( RetentionLevel nextLevel : pointingToCurrentLevel ){
                getLevelsAbove(nextLevel);
            }
        }
        updateCachingLevel( level );
    }
    
    private void updateCachingLevel( RetentionLevel level ){
        Optional<CachingLevel> cachingLevel = environment.cachingConfiguration().findLevelForLevelName(level.name());
        Optional<CachingLevel> oldCachingLevel = cachingConfig.findLevelForLevelName(level.name());
        if( !cachingLevel.isPresent() ){
            log.warn("RetentionLevel with unconfigured Cache " + level.name());            
        }
        else if( !caches.containsKey(level.name()) || !oldCachingLevel.isPresent() ){
            caches.put(level.name(), new LevelCache( environment, cachingLevel.get() ));
            if( levelIsAccessLevel( level ) && !evictionStrategyMap.containsKey(level.name())){
                evictionStrategyMap.put(level.name(), new NaiveLRUStrategy( level.name(), metricStorage, findAllFollowingLevelCaches(level) ,cachingLevel.get().visibleCacheSize()));
            }
            log.debug("created new Cache for " + level.name());
        }
        else if( oldCachingLevel.get().equals(cachingLevel.get()) ){
            log.debug("no changes in cachingLevel " +  level.name());
        }
        else if( oldCachingLevel.get().cacheLineWidth() != cachingLevel.get().cacheLineWidth() ){
            caches.remove(oldCachingLevel.get().name());
            caches.put(level.name(), new LevelCache( environment, cachingLevel.get() ));
            log.debug("dropped and created new cache for " + level.name());
        }
        else{
            log.debug("resizing level cache " + level.name());
            if( levelIsAccessLevel( level ) ){
                caches.get(oldCachingLevel.get().name()).resizeAccessLevel(cachingLevel.get(), evictionStrategyMap.get(level.name()) );
                evictionStrategyMap.get(level.name()).resize(cachingLevel.get().visibleCacheSize());
            }
            else{
                caches.get(oldCachingLevel.get().name()).resize(cachingLevel.get());
            }
        }
    }
    
    private boolean levelIsAccessLevel( RetentionLevel level ){
        return environment.retentions().getAllAccessLevels().contains(level);
    }
    
    private List<LevelCache> findAllFollowingLevelCaches( RetentionLevel level ){
        List<LevelCache> cachesList = new ArrayList<>();
        Optional<RetentionLevel> retentionLevel = Optional.of(level);
        while( retentionLevel.isPresent() && caches.containsKey(retentionLevel.get().name())){
            cachesList.add(caches.get(retentionLevel.get().name()));
            retentionLevel = environment.retentions().getNextLevel(retentionLevel.get());
        }
        return cachesList;
    }
    
    private Optional<EvictionStrategy> findEvictionStrategyForAccessLevel( RetentionLevel accessLevel){
        if( evictionStrategyMap.containsKey(accessLevel.name()) ){
            return Optional.of(evictionStrategyMap.get(accessLevel.name()));
        }
        return Optional.empty();
    }

    private Optional<MetricSet> doGetValues( String metricName, Interval interval, CachingLevel cachingLevel ){
        return caches.get(cachingLevel.name()).getValues(metricName, interval);
    }
    
    public Optional<MetricSet> getValues( String metricName, Interval interval ){
        Instant now = Clock.systemUTC().instant();
        Optional<RetentionLevel> accessLevel = environment.retentions().findAccessLevelForMetric(metricName);
        Optional<MetricSet> setWithHighestQoS = Optional.empty();
        double highestAvailableQoS = 0;
        if( !accessLevel.isPresent() ){
            return Optional.empty();
        }
        EvictionStrategy evictionStrategy = findEvictionStrategyForAccessLevel( accessLevel.get() ).get();
        evictionStrategy.accessing(metricName);
        while( accessLevel.isPresent() && caches.containsKey(accessLevel.get().name()) ){
            log.trace("Found accessLevel " + accessLevel.get().name());
            Optional<CachingLevel> cachingLevel = environment.cachingConfiguration().findLevelForLevelName(accessLevel.get().name());
            if( cachingLevel.isPresent() ){
                long cachingLevelStart = now.minusSeconds(cachingLevel.get().cacheLineWidth() * accessLevel.get().frequency()).getEpochSecond();
                if( cachingLevelStart < interval.start() ){
                    Optional<MetricSet> metricSet = doGetValues(metricName, interval, cachingLevel.get());
                    if( !metricSet.isPresent() ){
                        try {
                            createNewCacheLines(metricName).get();
                        } catch (InterruptedException | ExecutionException e) {
                            log.warn( "Exception while waiting for threads loading metrics", e );
                        }
                        metricSet = doGetValues(metricName, interval, cachingLevel.get());
                    }
                    if( metricSet.isPresent() ){
                        log.trace("metricSet contains {} timestamps", metricSet.get().size());
                        log.trace("accesslevel: {} | metricSet: {}", accessLevel.get().name(), metricSet.get());
                        OptionalDouble serviceAvailable = calculateQoS(interval, metricSet.get());
                        if( serviceAvailable.isPresent() && serviceAvailable.getAsDouble() >= environment.cachingConfiguration().qualityOfService() ){
                            log.trace("returning metricSet");
                            return metricSet;
                        }
                        else if( serviceAvailable.isPresent() && serviceAvailable.getAsDouble() > highestAvailableQoS ){
                            log.trace("new highestAvailable set");
                            highestAvailableQoS = serviceAvailable.getAsDouble();
                            setWithHighestQoS = metricSet;
                        }
                    }
                    else{
                        log.debug("no metricSet for cacheLevel {} metric {} found!", accessLevel.get().name(), metricName);
                    }
                }
            }
            accessLevel = environment.retentions().getNextLevel(accessLevel.get());
        }
        log.trace("service requested not available, returning highest available");
        return setWithHighestQoS;
    }
    
    private OptionalDouble calculateQoS(Interval interval, MetricSet metricSet){
        OptionalDouble qualityOfService = OptionalDouble.empty();
        long intervalSize = interval.end() - interval.start();
        double timestampsCalculated = intervalSize / metricSet.step();
        log.trace("intervalSize = {}; metricSet.step() = {}", intervalSize, metricSet.step());
        log.trace("metricSet should contain {} timestamps", timestampsCalculated);
        if(timestampsCalculated >= 1){
            qualityOfService = OptionalDouble.of(metricSet.size() / timestampsCalculated);
        }
        return qualityOfService;
    }
    
    private Future<?> createNewCacheLines( String metricName ){
        InitialDatabaseQuery query = new InitialDatabaseQuery( metricName );
        return databaseQueryPool.submit( Executors.callable( query ) );
    }

    public Map<String, Future<?>> preloadMetrics( Collection<String> metricNames ) {
        Map<String, Future<?>> preloadingMetrics = new HashMap<>();
        for ( String metricName : metricNames ) {
            preloadingMetrics.put( metricName, createNewCacheLines( metricName ) );
        }
        return preloadingMetrics;
    }
    
    public void put( Metric metric ){
        ProgramStateChanged.fireContextChangeToState("bifroest-bifroest-includeMetrics", "FindAccessLevelForMetric");
        Optional<RetentionLevel> level = environment.retentions().findAccessLevelForMetric(metric.name());
        while(level.isPresent() && caches.containsKey(level.get().name())){
            if(caches.get(level.get().name()).containsMetric(metric.name())){
                while( level.isPresent()  && caches.containsKey( level.get().name() ) ){
                    Optional<EvictionStrategy> evictionStrategy = findEvictionStrategyForAccessLevel(environment.retentions().findAccessLevelForMetric(metric).get());
                    if( !evictionStrategy.isPresent() ){
                        log.warn("metric {} with undefined evictionStrategy!", metric.name());
                        break;
                    }
                    caches.get(level.get().name()).put( metric, evictionStrategy.get() );
                    level = environment.retentions().getNextLevel(level.get());
                }
            }
            if(level.isPresent()){
                level = environment.retentions().getNextLevel(level.get());
            }
        }
    }

    public void shutdown() {
        for ( Map.Entry<String, LevelCache> entry : caches.entrySet() ) {
            entry.getValue().shutdown();
        }
        loadMetricsThreadPool.shutdown();
    }

    @Override 
    public void triggerEviction( String metricName ){
        log.info("manual eviction of {} triggered!", metricName);
        Optional<RetentionLevel> accessLevel = environment.retentions().findAccessLevelForMetric(metricName);
        if( accessLevel.isPresent() ){
            Optional<CachingLevel> cachingLevel = environment.cachingConfiguration().findLevelForLevelName(accessLevel.get().name());
            if( cachingLevel.isPresent() ){
                if( caches.get(cachingLevel.get().name()).containsMetric(metricName) ){
                    Optional<EvictionStrategy> evictionStrategy = findEvictionStrategyForAccessLevel(accessLevel.get());
                    if( evictionStrategy.isPresent() ){
                        evictionStrategy.get().mBeanTriggeredEviction(metricName);
                    }
                }
            }
        }
    }

    @Override
    public void clearLevelCache(String levelCacheName) {
        Optional<CachingLevel> cachingLevel = environment.cachingConfiguration().findLevelForLevelName(levelCacheName);
        if( !cachingLevel.isPresent() ){
            log.info("cachingLevel does not exist!");
            return;
        }
        if( !caches.containsKey(cachingLevel.get().name()) ){
            log.info("no cache for this level existing!");
            return;
        }
        Optional<RetentionLevel> retentionLevel = environment.retentions().getLevelForName(levelCacheName);
        if( !retentionLevel.isPresent() ){
            log.info("no retentionLevel for this levelCache! something is seriously broken!");
            return;
        }
        if( !levelIsAccessLevel(retentionLevel.get()) ){
            log.info("levelCache that is no accessLevelCache cannot be dropped!");
            return;
        }
        Optional<EvictionStrategy> evictionStrategy = findEvictionStrategyForAccessLevel(retentionLevel.get());
        if( !evictionStrategy.isPresent() ){
            log.info("no evictionStrategy found - there must be something wrong!");
            return;
        }
        List<String> metricsToClear = caches.get(levelCacheName).getMetrics();
        for( String metric : metricsToClear ){
            evictionStrategy.get().mBeanTriggeredEviction(metric);
        }
        log.info("cleared cache " + levelCacheName);
    }
    
    public Set<String> getAllMetricNames() {
        Set<String> metricNames = new HashSet<>();
        for (LevelCache levelCache : caches.values() ) {
            metricNames.addAll( levelCache.getContents().keySet() );
        }
        return metricNames;
    }

    private final class LoadMetrics implements Runnable {
        
        private final RetentionLevel retentionLevel;
        private final String metricName;
        private final List<Metric> databaseMetrics;
        private final Interval interval;


        public LoadMetrics( RetentionLevel retentionLevel, String metricName, List<Metric> databaseMetrics, Interval interval ) {
            this.retentionLevel = retentionLevel;
            this.metricName = metricName;
            this.databaseMetrics = databaseMetrics;
            this.interval = interval;
        }

        @Override
        public void run() {
            Optional<RetentionLevel> accessLevel = environment.retentions().findAccessLevelForMetric(metricName);
            if( accessLevel.isPresent() ){
                Optional<EvictionStrategy> evictionStrategy = findEvictionStrategyForAccessLevel(accessLevel.get());
                if( !evictionStrategy.isPresent() ){
                    log.warn("metric {} with undefined evictionStrategy!!", metricName);
                    return;
                }
                caches.get(retentionLevel.name()).putDatabaseMetrics(metricName, evictionStrategy.get(), databaseMetrics, interval);
                LoadMetricEvent.fire( metricName );
            }
        }

    }
    
    private final class InitialDatabaseQuery implements Runnable {
        
        private final String metricName;

        public InitialDatabaseQuery( String metricName ) {
            this.metricName = metricName;
        }

        @Override
        public void run() {
            Instant now = Clock.systemUTC().instant();
            Optional<RetentionLevel> accessLevel = environment.retentions().findAccessLevelForMetric(metricName);
            if( accessLevel.isPresent() ){
                if( caches.get(accessLevel.get().name()).containsMetric(metricName) ){
                    return;
                }
                Optional<RetentionLevel> highestLevel = accessLevel;
                while( highestLevel.isPresent() && highestLevel.get().next() != null ){
                    highestLevel = environment.retentions().getNextLevel(highestLevel.get());
                }
                Optional<CachingLevel> cachingLevel = environment.cachingConfiguration().findLevelForLevelName(highestLevel.get().name());
                if( cachingLevel.isPresent() ){
                    List<Future<?>> futures = new ArrayList<>();
                    ArrayList<Metric> databaseMetricsList = new ArrayList<>();
                    long intervalStart = now.minusSeconds(cachingLevel.get().cacheLineWidth() * highestLevel.get().frequency()).getEpochSecond();
                    Interval interval = new Interval( intervalStart, now.getEpochSecond() );
                    Stream<Metric> databaseMetrics = environment.cassandraAccessLayer().loadMetrics(metricName, interval);
                    Iterator<Metric> databaseMetricsIterator = databaseMetrics.iterator();
                    while( databaseMetricsIterator.hasNext() ){
                        Metric m = databaseMetricsIterator.next();
                        databaseMetricsList.add(m);
                    }
                    while( accessLevel.isPresent() ){
                        LoadMetrics loadMetrics = new LoadMetrics( accessLevel.get(), metricName, databaseMetricsList, interval );
                        futures.add(loadMetricsThreadPool.submit(Executors.callable(loadMetrics)));
                        accessLevel = environment.retentions().getNextLevel(accessLevel.get());
                    }
                    for ( Future<?> f : futures ) {
                        try {
                            f.get();
                        } catch (InterruptedException | ExecutionException e) {
                            log.warn( "Exception while waiting for insertion of metrics into cache", e );
                        }
                    }
                }
            }
            log.trace("all new cache lines created for metric {}", metricName);
        }

    }
    
}
