package com.goodgame.profiling.graphite_bifroest.systems.rebuilder.statistics;

import java.time.Clock;

import com.goodgame.profiling.commons.statistics.process.ProcessFinishedEvent;

public class RebuildFinishedEvent extends ProcessFinishedEvent {
	public RebuildFinishedEvent( Clock clock ) {
		super( clock, true);
	}
}
