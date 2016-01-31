package io.bifroest.bifroest.drain;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.bifroest.bifroest_client.metadata.NodeMetadata;
import io.bifroest.commons.model.Metric;
import io.bifroest.drains.AbstractBasicDrain;
import io.bifroest.bifroest.clustering.BifroestClustering;
import io.bifroest.bifroest.commands.statistics.DroppedMetricsEvent;
import io.bifroest.bifroest.systems.BifroestEnvironment;


public class TreeAndCacheDrain extends AbstractBasicDrain {
    private static final Logger log = LogManager.getLogger();
    private final BifroestEnvironment environment;
    
    public TreeAndCacheDrain( BifroestEnvironment environment ){
        this.environment = environment;
    }

    @Override
    public void output(List<Metric> metrics) throws IOException {
        BifroestClustering cluster = environment.getClustering();
        int countDroppedMetrics = 0;
        for( Metric metric : metrics ){
            NodeMetadata responsibleNode = cluster.whichNodeCachesMetric( metric.name() );
            if ( responsibleNode.equals( cluster.getMyOwnMetadata() ) ) {
                environment.getTree().addEntry( metric.name(), metric.timestamp() );
                environment.metricCache().put( metric );
            } else {
                log.debug( "Not responsible for this metric. Skipping metric." );
                countDroppedMetrics++;
            }
        }
        DroppedMetricsEvent.fire( "TreeAndCacheDrain", countDroppedMetrics );
    }
}