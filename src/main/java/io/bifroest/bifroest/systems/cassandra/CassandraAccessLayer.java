package io.bifroest.bifroest.systems.cassandra;

import java.util.stream.Stream;

import io.bifroest.commons.model.Interval;
import io.bifroest.commons.model.Metric;

public interface CassandraAccessLayer {

    Stream<Metric> loadMetrics( String name, Interval interval );

}
