package io.bifroest.bifroest.metric_cache;

import java.util.List;
import java.util.Optional;

public interface EvictionStrategy {
    void accessing( String metricName );
    Optional<String> whomShouldIEvict();
    void evict();
    void mBeanTriggeredEviction(String metricName);
    void resize( int newSize );
    void writeMostRequestedMetricsToFile( double percentage );
    List<String> getMostRequestedMetrics( double percentage );
}
