package com.goodgame.profiling.graphite_bifroest.systems.rebuilder;

import com.goodgame.profiling.commons.boot.interfaces.Environment;

public interface EnvironmentWithTreeRebuilder extends Environment {

	TreeRebuilder getRebuilder();
	long getNameRetention();

}
