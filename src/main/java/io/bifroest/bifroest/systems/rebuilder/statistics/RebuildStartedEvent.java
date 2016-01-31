package com.goodgame.profiling.graphite_bifroest.systems.rebuilder.statistics;

import java.time.Clock;

import com.goodgame.profiling.commons.statistics.process.ProcessStartedEvent;

public class RebuildStartedEvent extends ProcessStartedEvent {

	public RebuildStartedEvent( Clock clock ) {
		super( clock );
	}

}
