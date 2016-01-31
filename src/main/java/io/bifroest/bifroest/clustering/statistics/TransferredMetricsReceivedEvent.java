package io.bifroest.bifroest.clustering.statistics;

import java.util.Collection;

import io.bifroest.commons.statistics.eventbus.EventBusManager;

public class TransferredMetricsReceivedEvent {
    private final long count;

    private TransferredMetricsReceivedEvent( long count ) {
        this.count = count;
    }

    public long getCount() {
        return count;
    }

    public static void fire( Collection<String> metrics ) {
        EventBusManager.fire( new TransferredMetricsReceivedEvent( metrics.size() ) );
    }

    @Override
    public String toString() {
        return "TransferredMetricsReceivedEvent [count=" + count + "]";
    }
}