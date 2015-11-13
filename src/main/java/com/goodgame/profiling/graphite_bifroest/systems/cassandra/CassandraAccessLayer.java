package com.goodgame.profiling.graphite_bifroest.systems.cassandra;

import java.util.stream.Stream;

import com.goodgame.profiling.commons.model.Interval;
import com.goodgame.profiling.commons.model.Metric;

public interface CassandraAccessLayer {

    Stream<Metric> loadMetrics( String name, Interval interval );

}
