package com.goodgame.profiling.graphite_bifroest.commands.statistics;

import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;

public final class ForwardedMetricsEvent {
    private final String source;
    private final int numberOfMetrics;

    private ForwardedMetricsEvent( String source, int numberOfMetrics ) {
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
        return "ForwardedMetricsEvent{" + "source=" + source + ", numberOfMetrics=" + numberOfMetrics + '}';
    }

    public static void fire( String source, int numberOfMetrics ) {
        EventBusManager.fire( new ForwardedMetricsEvent( source, numberOfMetrics ) );
    }
}
