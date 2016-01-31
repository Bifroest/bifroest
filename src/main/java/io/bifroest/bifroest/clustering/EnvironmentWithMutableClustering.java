package io.bifroest.bifroest.clustering;

public interface EnvironmentWithMutableClustering extends EnvironmentWithClustering {
    void setClustering( BifroestClustering clustering );
}
