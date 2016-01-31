package com.goodgame.profiling.graphite_bifroest.metric_cache;

import com.goodgame.profiling.commons.model.Interval;

public interface CacheStrategy {
    boolean shouldICache( String metricName, Interval interval );
}
