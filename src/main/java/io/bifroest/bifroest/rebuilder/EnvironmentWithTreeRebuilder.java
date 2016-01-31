package io.bifroest.bifroest.rebuilder;

import io.bifroest.commons.boot.interfaces.Environment;

public interface EnvironmentWithTreeRebuilder extends Environment {

	TreeRebuilder getRebuilder();
	long getNameRetention();

}
