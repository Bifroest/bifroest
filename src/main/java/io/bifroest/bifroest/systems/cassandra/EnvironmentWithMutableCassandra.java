package io.bifroest.bifroest.systems.cassandra;


public interface EnvironmentWithMutableCassandra extends EnvironmentWithCassandra {

    void setCassandraAccessLayer( CassandraAccessLayer cassandra );

}
