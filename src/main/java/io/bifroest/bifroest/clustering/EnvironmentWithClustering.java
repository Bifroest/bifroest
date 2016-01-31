package com.goodgame.profiling.graphite_bifroest.clustering;

import com.goodgame.profiling.commons.boot.interfaces.Environment;

public interface EnvironmentWithClustering extends Environment {
    BifroestClustering getClustering();
}
