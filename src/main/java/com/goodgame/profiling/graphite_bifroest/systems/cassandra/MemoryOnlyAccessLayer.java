package com.goodgame.profiling.graphite_bifroest.systems.cassandra;

import java.util.stream.Stream;

import com.goodgame.profiling.commons.boot.interfaces.Environment;
import com.goodgame.profiling.commons.model.Interval;
import com.goodgame.profiling.commons.model.Metric;


public final class MemoryOnlyAccessLayer< E extends Environment > implements CassandraAccessLayer {
    public MemoryOnlyAccessLayer( E environment ){
    }

    @Override
    public Stream<Metric> loadMetrics( String name, Interval interval ) {
        return Stream.empty();
    }
}
