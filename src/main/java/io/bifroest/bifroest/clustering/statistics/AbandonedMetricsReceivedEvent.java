package com.goodgame.profiling.graphite_bifroest.clustering.statistics;

import java.util.Collection;

import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;

public final class AbandonedMetricsReceivedEvent {
    private final long count;

    private AbandonedMetricsReceivedEvent( long count ) {
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public static void fire( Collection<String> metrics ) {
        EventBusManager.fire( new AbandonedMetricsReceivedEvent( metrics.size() ) );
    }

    @Override
    public String toString() {
        return "AbandonedMetricsReceivedEvent [count=" + count + "]";
    }
}