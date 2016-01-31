package io.bifroest.bifroest.metric_cache;

public interface MetricCacheMBean {
	
	void triggerEviction( String metricName );
	void clearLevelCache( String levelCacheName );

}
