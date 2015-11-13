package com.goodgame.profiling.graphite_bifroest.commands.statistics.get_value;

import java.time.Clock;

import com.goodgame.profiling.graphite_bifroest.commands.statistics.ThreadWavingEvent;

public class GetValueOneMetricAggregated extends ThreadWavingEvent {
    public GetValueOneMetricAggregated( Clock clock ) {
        super( clock );
    }
}
