package io.bifroest.bifroest.rebuilder.statistics;

import java.time.Clock;

import io.bifroest.commons.statistics.process.ProcessFinishedEvent;

public class RebuildFinishedEvent extends ProcessFinishedEvent {
	public RebuildFinishedEvent( Clock clock ) {
		super( clock, true);
	}
}
