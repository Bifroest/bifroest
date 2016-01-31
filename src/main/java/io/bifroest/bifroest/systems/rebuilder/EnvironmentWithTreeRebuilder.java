package io.bifroest.bifroest.systems.rebuilder;

import io.bifroest.commons.boot.interfaces.Environment;

public interface EnvironmentWithTreeRebuilder extends Environment {

	TreeRebuilder getRebuilder();
	long getNameRetention();

}
