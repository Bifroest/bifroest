package com.goodgame.profiling.graphite_bifroest.metric_cache;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.goodgame.profiling.commons.model.Interval;
import com.goodgame.profiling.commons.model.Metric;
import com.goodgame.profiling.commons.statistics.cache.CacheTracker;
import com.goodgame.profiling.commons.statistics.jmx.MBeanManager;
import com.goodgame.profiling.graphite_bifroest.systems.BifroestEnvironment;
import com.goodgame.profiling.graphite_retentions.MetricSet;
import com.goodgame.profiling.graphite_retentions.RetentionLevel;

public class LevelCache implements LevelCacheMBean {
    private final Logger log;

    private static final Clock clock = Clock.systemUTC();

    private Map<String, OneMetricCache> cache = new ConcurrentHashMap<>();
    private Map<String, Instant> cacheLineEntered = new ConcurrentHashMap<>();

    private final BifroestEnvironment environment;

    private CachingLevel cachingLevel;

    private final BackendPool pool;

    private final String cacheName;

    private CacheTracker tracker;

    private final Object DAS_LOCK = new Object();


    public LevelCache( BifroestEnvironment environment, CachingLevel cachingLevel ) {
        this.log = LogManager.getLogger( this.getClass().getCanonicalName() + "." + cachingLevel.name() );
        this.environment = environment;
        this.cachingLevel = cachingLevel;
        this.cacheName = "MetricCache-" + cachingLevel.name();
        this.pool = new BackendPool( cacheName, cachingLevel.visibleCacheSize(), cachingLevel.totalCacheSize(), cachingLevel.cacheLineWidth() );
        this.tracker = CacheTracker.storingIn( "Caches", cacheName );

        MBeanManager.registerStandardMBean( this,
                LevelCache.class.getPackage().getName() + "." + cacheName + ":type=" + LevelCache.class.getSimpleName(),
                LevelCacheMBean.class );

    }

    public Optional<MetricSet> getValues( String metricName, Interval interval ) {
        OneMetricCache cacheLine = cache.get( metricName );
        if ( cacheLine == null ) {
            log.trace( "cacheline does not exist" );
            return Optional.empty();
        }
        else {
            log.trace( "cacheline exists" );
            return readFromCacheLine( metricName, interval, cacheLine );
        }
    }

    private Optional<MetricSet> readFromCacheLine( String metricName, Interval interval, OneMetricCache cacheline ) {
        MetricSet metrics = null;
        metrics = cacheline.getValues( interval );
        if ( !metrics.isEmpty() ) {
            log.trace( "metricSet for {} in interval from {} to {} present and contains {} timestamps", metricName, interval.start(), interval.end(), metrics.size() );
            tracker.cacheHit( cache.size(), cachingLevel.visibleCacheSize() );
        } else {
            tracker.cacheMiss( cache.size(), cachingLevel.visibleCacheSize() );
        }
        return Optional.ofNullable( metrics );
    }

    public void put( Metric metric, EvictionStrategy evictionStrategy ) {
        // Situation i: Metric is not cached and not requested, then this returns null,
        //              we don't add to the cache and that's it.
        // Situation ii: Metric is not cached, but it is currently being preloaded. Then
        //               This will return null at the moment, and thus we will drop the current
        //               datapoint. This is acceptable.
        // Situation iii: Someone else is currently writing to the one metric backend (e.g. the preloading
        //                AFTER it put the backend into the cache map). In this case,
        //                the write lock in the backend handles this situation, and also
        //                that stage of preloading is fast for a single metric (bounded by cache width)
        // Situation iv: No contention on the cache line, no problem.
        OneMetricCache targetCache = cache.get(metric.name());
        if ( targetCache != null ) {
            targetCache.addToCache( metric );
        }
    }


    public void putDatabaseMetrics( String metricName, EvictionStrategy evictionStrategy, List<Metric> databaseMetrics, Interval interval ) {
        // Core Problem in this method:
        // addDatabaseMetrics can take a long time, and DAS_LOCK locks the
        // entire level cache. For context, in a real config, there are around
        // 6 level caches with 6 level cache locks, and e.g. all preloading
        // threads must go through these locks.
        //
        // Thus, we need to keep the level cache lock (DAS_LOCK) as small as
        // possible. However! We need to get the write lock of the OneMetricCache
        // and we must avoid the possibility of someone snatching a read- or write
        // lock in the OneMetricCache.
        //
        // Thus the following horror of lock handshakes.

        OneMetricCache backendForThisMetric = null;
        long backendWriteStamp = Long.MIN_VALUE;

        try {
            synchronized(DAS_LOCK) {
                if ( !cache.containsKey( metricName ) ) {
                    if ( pool.isEmpty() ) {
                        evictionStrategy.evict();
                    }
                    Optional<OneMetricCacheBackend> backend = pool.getNextFree();
                    if ( backend.isPresent() ) {
                        OneMetricCache frontend = new OneMetricCache( environment, environment.retentions().getLevelForName( cachingLevel.name() ).get().frequency(), metricName, backend.get() );
                        pool.notifyFrontendCreated( frontend );
                        cacheLineEntered.put( metricName, clock.instant() );
                        cache.put( metricName, frontend );
                        tracker.update( cache.size(), cachingLevel.visibleCacheSize() );
                    }
                }

                backendForThisMetric = cache.get( metricName );

                // Concurrency hell begins here.
                // Important fact: OneMetricCache has no reference to this thread, thus
                // there is no chance for deadlocks
                if ( backendForThisMetric != null ) {
                    backendWriteStamp = backendForThisMetric.acquireWritelock();
                }
            } // Level Cache Lock ends here

            if ( backendForThisMetric != null ) {
                backendForThisMetric.addDatabaseMetrics( databaseMetrics, interval );
            }
        } finally {
            if ( backendForThisMetric != null ) {
                backendForThisMetric.releaseWritelock( backendWriteStamp );
            }
        }
    }

    public boolean containsMetric( String metricName ) {
        return cache.containsKey( metricName );
    }

    @Override
    public Map<String, String> getCacheLineAge() {
        Instant now = clock.instant();
        Map<String, String> ret = new HashMap<>();
        cacheLineEntered.forEach( ( metricName, when ) -> ret.put( metricName, Duration.between( when, now ).toString() ) );
        return ret;
    }

    public void evictCacheLine( String metricToRemove ) {
        cache.remove( metricToRemove );
        cacheLineEntered.remove( metricToRemove );
        tracker.cacheEviction();
        tracker.update(cache.size(), cachingLevel.visibleCacheSize());
        log.trace("evicted metric {} in LevelCache {}", metricToRemove, cachingLevel.name());
    }

    @Override
    public Map<String, String>getContents(){
        log.trace("entered getContents for LevelCache " + cachingLevel.name());
        Instant now = clock.instant();
        RetentionLevel rLevel = environment.retentions().getLevelForName(cachingLevel.name()).get();
        long cachingLevelStart = now.minusSeconds(cachingLevel.cacheLineWidth() * rLevel.frequency()).getEpochSecond();
        log.trace("cachingLevelStart = " + cachingLevelStart);
        Map<String, String> contents = new LinkedHashMap<String, String>();
        for( Entry<String, OneMetricCache> entry : cache.entrySet() ){
            log.trace("getContents - getValues from OneMetricCache for Metric" + entry.getKey());
            MetricSet values = entry.getValue().getValues(new Interval( cachingLevelStart, now.getEpochSecond()));
            log.trace("MetricSet for " + entry.getKey() + " has Size: " + values.size());
            Map<Long, Double> valuesMap = new LinkedHashMap<Long, Double>();
            double[] valueArray = values.values();
            int i = 0;
            long setTimestamp = values.interval().start();
            while(setTimestamp < values.interval().end()) {
                log.trace("setTimestamp = {}; endTimestamp = {}; values[{}] = {}", setTimestamp, values.interval().end(), i, valueArray[i]);
                valuesMap.put(setTimestamp, valueArray[i]);
                log.trace("valuesMap.put " + setTimestamp + " = " + valueArray[i]);
                setTimestamp = setTimestamp + values.step();
                i++;
            }
            log.trace("valuesMap for Metric {} completed!", entry.getKey());
            contents.put(entry.getKey(), valuesMap.toString());
        }
        log.trace("getContents contentsMap: " + contents.toString());

        return contents;
    }

    public void resize( CachingLevel newCachingLevel ) {
        log.trace( "entered: resize " + newCachingLevel.name() );
        cachingLevel = newCachingLevel;
        int toRemove = pool.resize( cachingLevel.visibleCacheSize(), cachingLevel.totalCacheSize(), cachingLevel.cacheLineWidth() );
        if ( toRemove > 0 ) {
            log.warn( "Something is broken!" );
        }
        tracker.update( cache.size(), cachingLevel.visibleCacheSize() );
    }

    public void resizeAccessLevel( CachingLevel newCachingLevel, EvictionStrategy evictionStrategy ) {
        log.trace( "entered: resizeAccessLevel " + newCachingLevel.name() );
        cachingLevel = newCachingLevel;
        int toRemove = pool.resize( cachingLevel.visibleCacheSize(), cachingLevel.totalCacheSize(), cachingLevel.cacheLineWidth() );
        while ( toRemove > 0 ) {
            Optional<String> cacheLineToEvict = evictionStrategy.whomShouldIEvict();
            if ( cacheLineToEvict.isPresent() ) {
                evictCacheLine( cacheLineToEvict.get() );
            }
            else {
                log.warn( "could not evict a cacheLine!" );
            }
            toRemove--;
        }
        tracker.update( cache.size(), cachingLevel.visibleCacheSize() );
    }
    
    public List<String> getMetrics(){
        List<String> metricsList = new LinkedList<String>();
        for( Entry<String, OneMetricCache> entry : cache.entrySet() ){
            metricsList.add(entry.getKey());
        }
        return metricsList;
    }

    public void shutdown() {
        pool.shutdown();
    }
}
