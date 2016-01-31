package io.bifroest.bifroest.cassandra;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import io.bifroest.commons.model.Interval;
import io.bifroest.commons.model.Metric;
import io.bifroest.retentions.RetentionTable;
import io.bifroest.retentions.bootloader.EnvironmentWithRetentionStrategy;

public class CassandraDatabase< E extends EnvironmentWithRetentionStrategy > {

    private static final Logger log = LogManager.getLogger();

    private static final String COL_NAME = "metric";
    private static final String COL_TIME = "timestamp";
    private static final String COL_VALUE = "value";

    private final String user;
    private final String pass;
    private final String keyspace;
    private final String[] hosts;

    private final E environment;
    private Cluster cluster;
    private Session session = null;

    public CassandraDatabase( String user, String pass, String keyspace, String[] hosts, E environment ) {
        this.environment = environment;
        this.user = user;
        this.pass = pass;
        this.keyspace = keyspace;
        this.hosts = hosts;
    }

    public void open() {
        if ( cluster == null || session == null ) {
            Builder builder = Cluster.builder();
            builder.addContactPoints( hosts );
            if ( user != null && pass != null && !user.isEmpty() && !pass.isEmpty() ) {
                builder = builder.withCredentials( user, pass );
            }
            cluster = builder.build();
            session = cluster.connect( keyspace );
        }
    }

    public void close() {
        if ( session != null ) {
            session.close();
            session = null;
        }
        if ( cluster != null ) {
            cluster.close();
            cluster = null;
        }
    }

    public Iterable<RetentionTable> loadTables() {
        List<RetentionTable> ret = new ArrayList<>();

        Collection<TableMetadata> metadatas = cluster.getMetadata().getKeyspace( keyspace ).getTables();

        for ( TableMetadata metadata : metadatas ) {
            if ( RetentionTable.TABLE_REGEX.matcher( metadata.getName() ).matches() ) {
                ret.add( new RetentionTable( metadata.getName(), environment.retentions() ) );
            } else {
                log.warn( "Table " + metadata.getName() + " doesn't match format." );
            }
        }

        return ret;
    }

    public Iterable<String> loadMetricNames( RetentionTable table ) {
        if ( session == null ) {
            open();
        }
        Statement stm = QueryBuilder.select().distinct().column( COL_NAME ).from( table.tableName() );
        final Iterator<Row> iter = session.execute( stm ).iterator();
        return new Iterable<String>() {

            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public String next() {
                        Row row = iter.next();
                        return row.getString( COL_NAME );
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public Long loadHighestTimestamp( RetentionTable table, String metricName ) {
        if ( session == null ) {
            open();
        }

        Statement stm = QueryBuilder
                .select( COL_TIME )
                .from( table.tableName() )
                .where( QueryBuilder.eq( COL_NAME, metricName ) )
                .orderBy( QueryBuilder.desc( COL_TIME ) )
                .limit( 1 );
        ResultSet results = session.execute( stm );
        Row row = results.one();
        if ( row == null ) {
            return null;
        } else {
            return row.getLong( COL_TIME );
        }
    }

    public Future<Collection<Metric>> loadMetrics( RetentionTable table, String name, Interval interval ) {
        if ( session == null ) {
            open();
        }
        Clause cName = QueryBuilder.eq( COL_NAME, name );
        // start inclusive, end exclusive
        Clause cBtm = QueryBuilder.gte( COL_TIME, interval.start() );
        Clause cTop = QueryBuilder.lt( COL_TIME, interval.end() );
        Statement stm = QueryBuilder.select().all().from( table.tableName() ).where( cName ).and( cBtm ).and( cTop );

        ResultSetFuture rsf = session.executeAsync( stm );

        return new Future<Collection<Metric>>() {
            private Collection<Metric> generateMetrics( ResultSet rs ) {
                Collection<Metric> results = new ArrayList<>( 16000 );
                for( Row row : rs ) {
                    results.add( new Metric( row.getString( COL_NAME ), row.getLong( COL_TIME ), row.getDouble( COL_VALUE ) ) );
                }
                return results;
            }

            @Override
            public Collection<Metric> get() throws InterruptedException, ExecutionException {
                return generateMetrics( rsf.get() );
            }

            @Override
            public Collection<Metric> get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException {
                return generateMetrics( rsf.get( timeout, unit ) );
            }

            @Override
            public boolean cancel( boolean mayInterruptIfRunning ) {
                return rsf.cancel( mayInterruptIfRunning );
            }

            @Override
            public boolean isCancelled() {
                return rsf.isCancelled();
            }

            @Override
            public boolean isDone() {
                return rsf.isDone();
            }
        };

    }

}
