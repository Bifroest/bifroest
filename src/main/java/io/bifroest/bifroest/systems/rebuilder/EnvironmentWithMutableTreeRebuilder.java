package com.goodgame.profiling.graphite_bifroest.systems.rebuilder;

public interface EnvironmentWithMutableTreeRebuilder extends EnvironmentWithTreeRebuilder {

	void setRebuilder( TreeRebuilder rebuilder );
	void setNameRetention( long nameRetention );

}
