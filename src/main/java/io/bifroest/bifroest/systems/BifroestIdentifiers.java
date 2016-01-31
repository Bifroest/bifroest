package io.bifroest.bifroest.systems;

public class BifroestIdentifiers {

    private BifroestIdentifiers() {
        // Utility class - avoid instantiation
    }

    public static final String CASSANDRA = "graphite.bifroest.cassandra";

    public static final String REBUILDER = "graphite.bifroest.recomputer";

    public static final String METRIC_CACHE = "graphite.bifroest.metric-cache";

    public static final String CLUSTERING = "graphite.bifroest.clustering";
}
