package io.bifroest.bifroest.clustering;

import io.bifroest.commons.boot.interfaces.Environment;

public interface EnvironmentWithClustering extends Environment {
    BifroestClustering getClustering();
}
