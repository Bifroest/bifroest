package io.bifroest.bifroest.cassandra;

import java.util.stream.Stream;

import io.bifroest.commons.boot.interfaces.Environment;
import io.bifroest.commons.model.Interval;
import io.bifroest.commons.model.Metric;


public final class MemoryOnlyAccessLayer< E extends Environment > implements CassandraAccessLayer {
    public MemoryOnlyAccessLayer( E environment ){
    }

    @Override
    public Stream<Metric> loadMetrics( String name, Interval interval ) {
        return Stream.empty();
    }
}
