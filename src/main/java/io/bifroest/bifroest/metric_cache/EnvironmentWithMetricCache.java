package io.bifroest.bifroest.metric_cache;

import io.bifroest.commons.boot.interfaces.Environment;

public interface EnvironmentWithMetricCache extends Environment {
    public MetricCache metricCache();
    public CachingConfiguration cachingConfiguration();
}
