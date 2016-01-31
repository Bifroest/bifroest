package io.bifroest.bifroest.rebuilder;

public interface EnvironmentWithMutableTreeRebuilder extends EnvironmentWithTreeRebuilder {

	void setRebuilder( TreeRebuilder rebuilder );
	void setNameRetention( long nameRetention );

}
