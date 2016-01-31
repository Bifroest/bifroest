package io.bifroest.bifroest.commands.statistics.get_value;

import java.time.Clock;

import io.bifroest.bifroest.commands.statistics.ThreadWavingEvent;

public class GetValueOneMetricAggregated extends ThreadWavingEvent {
    public GetValueOneMetricAggregated( Clock clock ) {
        super( clock );
    }
}
