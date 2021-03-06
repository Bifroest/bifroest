package io.bifroest.bifroest.metric_cache.cache_strategy;

import io.bifroest.commons.model.Interval;
import io.bifroest.bifroest.metric_cache.CacheStrategy;

public class AlwaysCacheStrategy implements CacheStrategy {
    @Override
    public boolean shouldICache( String metricName, Interval interval ) {
        return true;
    }
}
