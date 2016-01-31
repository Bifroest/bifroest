package com.goodgame.profiling.graphite_bifroest.clustering;

public interface EnvironmentWithMutableClustering extends EnvironmentWithClustering {
    void setClustering( BifroestClustering clustering );
}
