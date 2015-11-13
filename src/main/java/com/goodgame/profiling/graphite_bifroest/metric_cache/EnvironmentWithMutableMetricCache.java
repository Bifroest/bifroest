package com.goodgame.profiling.graphite_bifroest.metric_cache;

public interface EnvironmentWithMutableMetricCache extends EnvironmentWithMetricCache {
    public void setMetricCache( MetricCache cache );
    public void setCachingConfiguration( CachingConfiguration configuration );
}
