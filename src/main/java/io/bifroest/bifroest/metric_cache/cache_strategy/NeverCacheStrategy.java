package com.goodgame.profiling.graphite_bifroest.metric_cache.cache_strategy;

import com.goodgame.profiling.commons.model.Interval;
import com.goodgame.profiling.graphite_bifroest.metric_cache.CacheStrategy;

public class NeverCacheStrategy implements CacheStrategy {
    @Override
    public boolean shouldICache( String metricName, Interval interval ) {
        return false;
    }
}
