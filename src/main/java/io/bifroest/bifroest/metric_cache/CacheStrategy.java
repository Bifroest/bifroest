package io.bifroest.bifroest.metric_cache;

import io.bifroest.commons.model.Interval;

public interface CacheStrategy {
    boolean shouldICache( String metricName, Interval interval );
}
