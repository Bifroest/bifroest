package com.goodgame.profiling.graphite_bifroest.systems.cassandra;


public interface EnvironmentWithMutableCassandra extends EnvironmentWithCassandra {

    void setCassandraAccessLayer( CassandraAccessLayer cassandra );

}
