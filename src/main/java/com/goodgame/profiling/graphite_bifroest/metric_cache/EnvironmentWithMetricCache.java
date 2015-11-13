package com.goodgame.profiling.graphite_bifroest.metric_cache;

import com.goodgame.profiling.commons.boot.interfaces.Environment;

public interface EnvironmentWithMetricCache extends Environment {
    public MetricCache metricCache();
    public CachingConfiguration cachingConfiguration();
}
