package com.goodgame.profiling.graphite_bifroest.metric_cache.statistics;

public final class RaceConditionTriggeredEvent {
    private final String cacheName;

    public RaceConditionTriggeredEvent( String cacheName ) {
        this.cacheName = cacheName;
    }

    public String cacheName() {
        return cacheName;
    }
}
