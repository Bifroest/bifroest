package com.goodgame.profiling.graphite_bifroest.systems.prefixtree;


public interface EnvironmentWithMutablePrefixTree extends EnvironmentWithPrefixTree {

	void setTree( PrefixTree tree );

}
