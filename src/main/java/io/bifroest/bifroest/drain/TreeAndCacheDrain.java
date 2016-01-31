package com.goodgame.profiling.graphite_bifroest.drain;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.commons.model.Metric;
import com.goodgame.profiling.drains.AbstractBasicDrain;
import com.goodgame.profiling.graphite_bifroest.clustering.BifroestClustering;
import com.goodgame.profiling.graphite_bifroest.commands.statistics.DroppedMetricsEvent;
import com.goodgame.profiling.graphite_bifroest.systems.BifroestEnvironment;


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