package io.bifroest.bifroest.metric_cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.StampedLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.bifroest.commons.model.Interval;
import io.bifroest.commons.model.Metric;
import io.bifroest.bifroest.systems.BifroestEnvironment;
import io.bifroest.retentions.Aggregator;
import io.bifroest.retentions.MetricSet;

public final class OneMetricCache {
    private static final int RETRY_COUNT = 5;

    private static final Logger log = LogManager.getLogger();
    private final BifroestEnvironment environment;

    private final StampedLock lock;

    private final List<Metric> aggregationCache;

    private final String metricName;
    private final OneMetricCacheBackend backend;

    private final long frequency;

    public OneMetricCache( BifroestEnvironment environment, long frequency, String metricName, OneMetricCacheBackend backend ) {
        this.lock = new StampedLock();
        this.aggregationCache = new ArrayList<Metric>();
        this.metricName = metricName;
        this.backend = backend;

        this.environment = environment;

        this.frequency = frequency;
    }

    private int timestampToIndex( long timestamp ) {
        long bucketIndex = timestamp / frequency;
        return (int)bucketIndex;
    }

    private void doAddToCache( Iterable<Metric> metrics ) {
        for ( Metric metric : metrics ) {
            int index = timestampToIndex( metric.timestamp() );
            this.backend.put( index, metric.value() );
        }
    }

    public void addToCache( Metric metric ) {
        log.trace( "adding " + metric );
        long stamp = lock.writeLock();
        try {
            aggregationCache.add( metric );
            long earliestTimestamp = Long.MAX_VALUE;
            long latestTimestamp = 0;
            for ( Metric m : aggregationCache ) {
                earliestTimestamp = Math.min( earliestTimestamp, m.timestamp() );
                latestTimestamp = Math.max( latestTimestamp, m.timestamp() );
            }
            earliestTimestamp = Aggregator.alignTo( earliestTimestamp, frequency );
            Interval interval = new Interval( earliestTimestamp, earliestTimestamp + frequency );
            if ( interval.end() < latestTimestamp ) {
                Collection<Metric> newMetrics = Aggregator.aggregate( metricName, aggregationCache, interval, frequency, environment.retentions(), true );
                doAddToCache( newMetrics );
                interval = new Interval( interval.end(), interval.end() + frequency );
            }
        } finally {
            lock.unlockWrite( stamp );
        }
    }

    public long acquireWritelock() {
        return lock.writeLock();
    }

    public void releaseWritelock(long stamp) {
        lock.unlockWrite(stamp);
    }

    public void addDatabaseMetrics( List<Metric> databaseMetrics, Interval interval ) {
        if ( !lock.isWriteLocked() ) {
            throw new IllegalStateException("OneMetricCache MUST be locked through acquireWritelock before this methods" );
        }
        interval = Aggregator.alignInterval( interval, frequency );
        Collection<Metric> newMetrics = Aggregator.aggregate( metricName, databaseMetrics, interval, frequency, environment.retentions() );
        doAddToCache( newMetrics );
    }

    private MetricSet doGetValues( Interval interval ) {
        Interval alignedInterval = Aggregator.alignInterval( interval, frequency );
        MetricSet metricSet = new MetricSet( metricName, alignedInterval, frequency );
        log.trace( "OneMetricCache | aligned Interval -> start: {}, end:{}", alignedInterval.start(), alignedInterval.end() );
        int startOfSearch = Math.max( timestampToIndex( alignedInterval.start() ), backend.lowerBound() );
        log.trace( "timestampToIndex(alignedInterval.start()) = {}, backend.lowerBound() = {}", timestampToIndex( alignedInterval.start() ), backend.lowerBound() );
        int endOfSearch = Math.min( timestampToIndex( alignedInterval.end() ), backend.upperBound() );
        log.trace( "timestampToIndex(alignedInterval.end()) = {}, backend.upperBound() = {}", timestampToIndex( alignedInterval.end() ), backend.upperBound() );
        log.trace( "Trying to get {} values from backend.", endOfSearch - startOfSearch );
        for ( int index = startOfSearch; index < endOfSearch; index++ ) {
            double potentialValue = backend.get( index );
            metricSet.setValue( index - startOfSearch, potentialValue );
        }
        return metricSet;
    }

    public MetricSet getValues( Interval interval ) {
        for ( int i = 0; i < RETRY_COUNT; i++ ) {
            long stamp = lock.tryOptimisticRead();

            MetricSet ret = doGetValues( interval );
            if ( lock.validate( stamp ) ) {
                return ret;
            }
        }

        long stamp = lock.readLock();
        MetricSet ret;
        try {
            ret = doGetValues( interval );
        } finally {
            lock.unlockRead( stamp );
        }

        return ret;
    }

    /* evil package private method: leak backend, so we can reuse it after this object dies */
    OneMetricCacheBackend getBackend() {
        return backend;
    }
}
