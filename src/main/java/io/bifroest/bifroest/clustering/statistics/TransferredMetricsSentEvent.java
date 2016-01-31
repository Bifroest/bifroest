package io.bifroest.bifroest.clustering.statistics;

import java.util.Collection;

import io.bifroest.commons.statistics.eventbus.EventBusManager;

public final class TransferredMetricsSentEvent {
    private final long count;

    private TransferredMetricsSentEvent( long count ) {
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public static void fire( Collection<String> metrics ) {
        EventBusManager.fire( new TransferredMetricsSentEvent( metrics.size() ) );
    }

    @Override
    public String toString() {
        return "TransferredMetricsSentEvent [count=" + count + "]";
    }
}
