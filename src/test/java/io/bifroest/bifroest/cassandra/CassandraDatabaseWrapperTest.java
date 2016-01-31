package io.bifroest.bifroest.cassandra;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.not;

import io.bifroest.bifroest.cassandra.CassandraAccessLayer;
import io.bifroest.bifroest.cassandra.CassandraDatabase;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.lang3.concurrent.ConcurrentUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import io.bifroest.commons.model.Interval;
import io.bifroest.commons.model.Metric;
import io.bifroest.bifroest.cassandra.wrapper.CassandraDatabaseWrapper;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.RetentionLevel;
import io.bifroest.retentions.RetentionTable;
import io.bifroest.retentions.bootloader.EnvironmentWithRetentionStrategy;

public class CassandraDatabaseWrapperTest {
    @Mock
    EnvironmentWithRetentionStrategy environment;

    @Mock
    CassandraDatabase<EnvironmentWithRetentionStrategy> database;

    @Mock
    RetentionConfiguration retentionConfig;

    CassandraAccessLayer cassandra;

    RetentionLevel precise, hourly, precise2;
    RetentionTable lvl1blk1, lvl1blk3, dangling, lvl2blk0, lvl2blk1, lvl2blk2, lvl3blk1;
    Metric metric01, metric02, metric03, metric04, metric05, metric06, metric07, metric08, metric09, metric10, metric11;

    @Before
    public void createMocks() {
        MockitoAnnotations.initMocks( this );
        long timestamp = 1000000000;
        when(environment.retentions()).thenReturn(retentionConfig);

        cassandra = new CassandraDatabaseWrapper<>( database, environment );

        // Levels and strategy
        precise = new RetentionLevel( "precise", 300, 3, 60 * 60, "hourly" );
        precise2 = new RetentionLevel( "precise2", 300, 3, 60 * 60, "hourly" );
        hourly = new RetentionLevel( "hourly", 60 * 60, 4, 60 * 60 * 24 * 7, null );

        // Head block
        long index = precise.indexOf( timestamp );
        lvl1blk1 = new RetentionTable(  precise, index );
        // One is missing
        lvl1blk3 = new RetentionTable(  precise, index - 2 );
        // Dangling block
        dangling = new RetentionTable( precise, index - 300 );
        //Head block in alternate first level
        index = precise2.indexOf( timestamp );
        lvl3blk1 = new RetentionTable( precise2, index);
        // Random misplaced block - should be ignored
        index = hourly.indexOf( timestamp );
        lvl2blk0 = new RetentionTable( hourly, index );
        // Second level head block
        index = hourly.indexOf( lvl1blk3.getInterval().start() );
        lvl2blk1 = new RetentionTable( hourly, index );
        // Second level regular block
        lvl2blk2 = new RetentionTable( hourly, index - 1 );

        when( retentionConfig.findAccessLevelForMetric( anyString() ) ).thenReturn( Optional.of(precise ));
        when( retentionConfig.getNextLevel(precise)).thenReturn(Optional.of(hourly));
        when( retentionConfig.getNextLevel(precise2)).thenReturn(Optional.of(hourly));
        when( retentionConfig.getNextLevel(hourly)).thenReturn(Optional.empty());

        when( database.loadMetricNames( lvl1blk1 ) ).thenReturn( Arrays.asList( "name01", "name02", "name05" ) );
        when( database.loadMetricNames( lvl1blk3 ) ).thenReturn( Arrays.asList( "name03", "name02" ) );
        when( database.loadMetricNames( dangling ) ).thenReturn( Arrays.asList( "name01", "name04", "name05", "name06" ) );
        when( database.loadMetricNames( lvl2blk1 ) ).thenReturn( Arrays.asList( "name05", "name03", "name05", "name06" ) );
        when( database.loadMetricNames( lvl2blk2 ) ).thenReturn( Collections.<String> emptyList() );
        when( database.loadMetricNames( lvl3blk1 ) ).thenReturn( Arrays.asList( "name02" ) );

        metric01 = new Metric( "name02", lvl1blk1.getInterval().end() - 500, 7 );
        metric02 = new Metric( "name02", lvl1blk1.getInterval().end() - 800, 42 );
        metric03 = new Metric( "name02", lvl1blk3.getInterval().end() - 500, 13 );
        metric04 = new Metric( "name03", lvl1blk3.getInterval().end() - 800, 1 );
        metric05 = new Metric( "name03", lvl2blk2.getInterval().end() - 500, 2 );
        // testLoadMetricAcrossLevelsWithGabs()
        metric06 = new Metric( "name05", lvl1blk1.getInterval().end() - 500, 51 );
        metric07 = new Metric( "name05", dangling.getInterval().start() + 1, 53 );
        metric08 = new Metric( "name05", lvl2blk1.getInterval().start() + 1, 52 );
        // testLoadMetricAcrossLevelsWithFirstMetricIsInLevelTwo()
        metric09 = new Metric( "name06", dangling.getInterval().start() + 1, 61 );
        metric10 = new Metric( "name06", lvl2blk1.getInterval().end() - 50, 62 );
        //testLoadMetricMultipleReadLevels()
        metric11 = new Metric( "name02", lvl3blk1.getInterval().end() - 200, 5 );

        when( database.loadMetrics( any( RetentionTable.class ), any( String.class ), any( Interval.class ) ) ).thenAnswer(new Answer<Future<Collection<Metric>>>() {
            @Override
            public Future<Collection<Metric>> answer(InvocationOnMock invocation) throws Throwable {
                return ConcurrentUtils.constantFuture( Collections.emptyList() );
            }
        });

        // Mocks
        List<RetentionTable> list = Arrays.asList( lvl1blk1, lvl1blk3, dangling, lvl2blk0, lvl2blk1, lvl2blk2, lvl3blk1 );
        Collections.shuffle( list );
        when( database.loadTables() ).thenReturn( list );
    }

    private void initLoadMetrics( String name, RetentionTable table, Metric... metrics ) {
        when( database.loadMetrics( eq( table ), eq( name ), argThat( new IntervalMatcher( metrics ) ) ) ).thenAnswer(new Answer<Future<Collection<Metric>>>() {
            @Override
            public Future<Collection<Metric>> answer(InvocationOnMock invocation) throws Throwable {
                return ConcurrentUtils.constantFuture( Arrays.asList( metrics ) );
            }
        });
    }

    private List<Metric> loadMetrics( String name, RetentionTable... tables ) {
        long start = Long.MAX_VALUE, end = Long.MIN_VALUE;
        for ( int i = 0; i < tables.length; i++ ) {
            Interval interval = tables[i].getInterval();
            start = Math.min( start, interval.start() );
            end = Math.max( end, interval.end() );
        }
        return cassandra.loadMetrics( name, new Interval( start, end ) ).collect(Collectors.toList());
    }

    @Test
    public void testLoadMetricsInOneTable() {
        initLoadMetrics( "name02", lvl1blk1, metric01, metric02 );
        List<Metric> metrics = loadMetrics( "name02", lvl1blk1 );
        
        assertThat(metrics, hasItems(metric01, metric02));
        assertThat(metrics, not(hasItem(metric03)));
    }

    @Test
    public void testLoadMetricsInOneLevel() {
        initLoadMetrics( "name02", lvl1blk1, metric01, metric02 );
        initLoadMetrics( "name02", lvl1blk3, metric03 );
        List<Metric> metrics = loadMetrics( "name02", lvl1blk1, lvl1blk3 );
        
        assertThat(metrics, hasItems(metric01, metric02, metric03));
    }

    @Ignore
    @Test
    public void testLoadMetricsAcrossLevels() {
        initLoadMetrics( "name03", lvl1blk3, metric04 );
        initLoadMetrics( "name03", lvl2blk2, metric05 );
        List<Metric> metrics = loadMetrics( "name03", lvl1blk3, lvl2blk2 );
        
        assertThat(metrics, hasItems(metric04, metric05));
    }

    @Test
    public void testLoadMetricAcrossLevelsWithGaps() {
        initLoadMetrics( "name05", lvl1blk1, metric06 );
        initLoadMetrics( "name05", dangling, metric07 );
        initLoadMetrics( "name05", lvl2blk1, metric08 );
        List<Metric> metrics = loadMetrics( "name05", lvl1blk1, dangling, lvl2blk1 );
        
        assertThat(metrics, hasItems(metric06, metric07, metric08));
    }

    @Test
    public void testLoadMetricAcrossLevelsWithFirstMetricIsInLevelTwo() {
        initLoadMetrics( "name06", dangling, metric09 );
        initLoadMetrics( "name06", lvl2blk1, metric10 );
        List<Metric> metrics = loadMetrics( "name06", dangling, lvl2blk1 );
        
        assertThat(metrics, hasItems(metric09, metric10));
    }
}
