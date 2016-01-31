package com.goodgame.profiling.graphite_bifroest.metric_cache.eviction_strategy;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.collections4.MapIterator;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.goodgame.profiling.graphite_bifroest.metric_cache.EvictionStrategy;
import com.goodgame.profiling.graphite_bifroest.metric_cache.LevelCache;

public class NaiveLRUStrategy implements EvictionStrategy {
    private static final Logger log = LogManager.getLogger();

    private static final Object THE_OBJECT = new Object();
    private LRUMap<String, Object> decider;

    private List<LevelCache> caches;
    private String accessLevelName;

    private final String metricStorage;

    public NaiveLRUStrategy( String accessLevel, String metricStorage, List<LevelCache> caches, int size ){
        this.accessLevelName = accessLevel;
        this.metricStorage = metricStorage;
        this.decider = new LRUMap<String, Object>(size);
        this.caches = caches;
    }

    @Override
    public synchronized void accessing( String metricName ) {
        decider.put( metricName, THE_OBJECT );
    }

    @Override
    public synchronized Optional<String> whomShouldIEvict() {
        if(decider.isEmpty()){
            return Optional.empty();
        }
        String ret = decider.firstKey();
        decider.remove( ret );
        return Optional.of(ret);
    }

    public void resize ( int newSize ){
        LRUMap<String, Object> newdecider = new LRUMap<String, Object>( newSize );
        MapIterator<String, Object> iterator = decider.mapIterator();
        while( iterator.hasNext() ){
            String entry = iterator.next();
            newdecider.put(entry, THE_OBJECT);
        }

        this.decider = newdecider;
    }

    public void evict(){
        Optional<String> metricToRemove = whomShouldIEvict();
        if( metricToRemove.isPresent() ){
            for( LevelCache cache : caches ){
                cache.evictCacheLine(metricToRemove.get());
            }
        }
    }

    public void mBeanTriggeredEviction( String metricName ){
        decider.remove(metricName);
        for( LevelCache cache : caches ){
            cache.evictCacheLine(metricName);
        }
    }

    public void writeMostRequestedMetricsToFile( double percentage ){
        if( decider.isEmpty() ){
            log.debug("Got told to write metrics to file, but had no metrics. Not writing anything");
            return;
        }
        int numberOfMetrics = (int) (decider.maxSize() * percentage);
        if( numberOfMetrics > decider.size() ){
            numberOfMetrics = decider.size();
        }
        if( numberOfMetrics < 1 ){
            log.debug("Got told to write metrics to file, but had 1 or less metrics. Not writing anything");
            return;
        }
        Path dir = Paths.get( metricStorage );
        try {
            Files.createDirectories( dir );
        } catch ( FileAlreadyExistsException e ) {
            // ignore
        } catch( IOException e ) {
            log.warn( "Cannot create directory " + dir );
            return;
        }
        Path storageFile = Paths.get( metricStorage, accessLevelName );
        log.debug( "Writing to file " + storageFile );
        try (OutputStreamWriter toStorageFile = new OutputStreamWriter( new GZIPOutputStream( Files.newOutputStream( storageFile ) ), Charset.forName("UTF-8") )) {
            toStorageFile.write(numberOfMetrics + "\n"); //first line always contains the number of metrics in the file!
            String metricToWrite = decider.lastKey();
            for( int i = 0; i < numberOfMetrics; i++ ){
                toStorageFile.write(metricToWrite + "\n");
                metricToWrite = decider.previousKey(metricToWrite);
            }

            log.info("successfully wrote file for " + accessLevelName);
        } catch (IOException e) {
            log.warn("could not write metrics to file for " + accessLevelName , e);
        }
    }

    public List<String> getMostRequestedMetrics( double percentage ){
        List<String> mostRequestedMetrics = new ArrayList<>();
        if( decider.isEmpty() ){
            log.debug("No metrics found. Returning empty List.");
            return mostRequestedMetrics;
        }
        int numberOfMetrics = (int) ( decider.maxSize() * percentage );
        if( numberOfMetrics > decider.size() ){
            numberOfMetrics = decider.size();
        }
        String metricToAdd = decider.lastKey();
        for( int i = 0; i < numberOfMetrics; i++ ){
            mostRequestedMetrics.add( metricToAdd );
            metricToAdd = decider.previousKey( metricToAdd );
        }
        return mostRequestedMetrics;
    }
}
