package com.goodgame.profiling.graphite_bifroest.systems.prefixtree;

import com.goodgame.profiling.commons.boot.interfaces.Environment;

public interface EnvironmentWithPrefixTree extends Environment {

	PrefixTree getTree();

}
