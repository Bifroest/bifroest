package io.bifroest.bifroest.rebuilder.statistics;

import java.time.Clock;

import io.bifroest.commons.statistics.process.ProcessStartedEvent;

public class RebuildStartedEvent extends ProcessStartedEvent {

	public RebuildStartedEvent( Clock clock ) {
		super( clock );
	}

}
