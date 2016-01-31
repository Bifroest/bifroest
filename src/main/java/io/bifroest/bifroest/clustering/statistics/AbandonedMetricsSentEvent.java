package io.bifroest.bifroest.clustering.statistics;

import java.util.Collection;

import io.bifroest.commons.statistics.eventbus.EventBusManager;

public class AbandonedMetricsSentEvent {
    private final long count;

    private AbandonedMetricsSentEvent( long count ) {
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public static void fire( Collection<String> metrics ) {
        EventBusManager.fire( new AbandonedMetricsSentEvent( metrics.size() ) );
    }

    @Override
    public String toString() {
        return "AbandonedMetricsSentEvent [count=" + count + "]";
    }
}
