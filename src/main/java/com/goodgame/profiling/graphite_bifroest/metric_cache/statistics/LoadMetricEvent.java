package com.goodgame.profiling.graphite_bifroest.metric_cache.statistics;

import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;

public final class LoadMetricEvent {
    private final String metric;

    private LoadMetricEvent( String metric ) {
        this.metric = metric;
    }

    public String getMetric() {
        return metric;
    }

    public static void fire( String metric ) {
        EventBusManager.fire( new LoadMetricEvent( metric ) );
    }

    @Override
    public String toString() {
        return "LoadMetricEvent [metric=" + metric + "]";
    }
}
