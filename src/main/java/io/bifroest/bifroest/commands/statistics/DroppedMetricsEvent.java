package com.goodgame.profiling.graphite_bifroest.commands.statistics;

import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;

public final class DroppedMetricsEvent {
    private final String source;
    private final int numberOfMetrics;

    private DroppedMetricsEvent( String source, int numberOfMetrics ) {
        this.source = source;
        this.numberOfMetrics = numberOfMetrics;
    }

    public String getSource() {
        return source;
    }

    public int getNumberOfMetrics() {
        return numberOfMetrics;
    }

    @Override
    public String toString() {
        return "DroppedMetricsEvent{" + "source=" + source + ", numberOfMetrics=" + numberOfMetrics + '}';
    }

    public static void fire( String source, int numberOfMetrics ) {
        if ( numberOfMetrics > 0 ) {
            EventBusManager.fire( new DroppedMetricsEvent( source, numberOfMetrics ) );
        }
    }
}
