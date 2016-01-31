package io.bifroest.bifroest.prefixtree;


public interface EnvironmentWithMutablePrefixTree extends EnvironmentWithPrefixTree {

	void setTree( PrefixTree tree );

}
