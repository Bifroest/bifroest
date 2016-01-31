package io.bifroest.bifroest.prefixtree;

import io.bifroest.commons.boot.interfaces.Environment;

public interface EnvironmentWithPrefixTree extends Environment {

	PrefixTree getTree();

}
