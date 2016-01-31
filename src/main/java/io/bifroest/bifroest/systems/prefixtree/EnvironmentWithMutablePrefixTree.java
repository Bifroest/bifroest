package io.bifroest.bifroest.systems.prefixtree;


public interface EnvironmentWithMutablePrefixTree extends EnvironmentWithPrefixTree {

	void setTree( PrefixTree tree );

}
