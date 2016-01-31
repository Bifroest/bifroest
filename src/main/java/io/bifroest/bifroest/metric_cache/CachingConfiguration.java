package io.bifroest.bifroest.metric_cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Optional;

import io.bifroest.commons.configuration.InvalidConfigurationException;
import io.bifroest.retentions.RetentionConfiguration;
import io.bifroest.retentions.RetentionLevel;

public class CachingConfiguration {
    
     private final RetentionConfiguration retentionConfig;
     private final Map<String, CachingLevel> levels;
     
     private final String metricStorage;
     private double qualityOfService;
     private double percentageOfMetricsToSave;
     private double percentageOfMetricsToTransfer;
     private final int numberOfDatabaseQueryThreads;
     private final int numberOfWriteToCacheThreads;
     
    
    public CachingConfiguration(RetentionConfiguration retentionConfig, String metricStorage, double qualityofService, double percentageOfMetricsToSave, double percentageOfMetricsToTransfer, int numberOfDatabaseQueryThreads, int numberOfWriteToCacheThreads){
        this.metricStorage = metricStorage;
        this.retentionConfig = retentionConfig;
        this.levels = new HashMap<>();
        this.qualityOfService = qualityofService;
        this.percentageOfMetricsToSave = percentageOfMetricsToSave;
        this.percentageOfMetricsToTransfer = percentageOfMetricsToTransfer;
        this.numberOfDatabaseQueryThreads = numberOfDatabaseQueryThreads;
        this.numberOfWriteToCacheThreads = numberOfWriteToCacheThreads;
    }
    
    public void addLevel( CachingLevel level ){
        levels.put( level.name(), level);
    }
    
    public void correctCacheSizes() throws InvalidConfigurationException{
        Collection<RetentionLevel> allLevels = retentionConfig.getAllLevels();
        List<RetentionLevel> list = new ArrayList<RetentionLevel>();
        for(RetentionLevel level : allLevels) {            
            if( level.next() == null ){
                list.add(level);
            }
        }        
        for( RetentionLevel level : list ){
            getLevelsAbove(level);
        }
    }
    
    private void getLevelsAbove( RetentionLevel level ) throws InvalidConfigurationException{
        Collection<RetentionLevel> list = retentionConfig.getAllLevels();
        List<RetentionLevel> pointingToCurrentLevel = new LinkedList<RetentionLevel>();
        for( RetentionLevel rlevel : list ){
            if( rlevel.next() != null ){
                if( rlevel.next().equals(level.name()) ){
                    pointingToCurrentLevel.add( rlevel );
                }
            }
        }
        if( !pointingToCurrentLevel.isEmpty() ){
            List<RetentionLevel> pointingToLevelAboveCurrentLevel = new LinkedList<RetentionLevel>();
            for( RetentionLevel rlevel : pointingToCurrentLevel ){
                for( RetentionLevel l : list ){
                    if( l.next() != null ){
                        if( l.next().equals(rlevel.name()) ){
                            pointingToLevelAboveCurrentLevel.add(l);
                        }
                    }
                }
            }
            if( !pointingToLevelAboveCurrentLevel.isEmpty() ){
                for( RetentionLevel nextLevel : pointingToCurrentLevel ){
                    getLevelsAbove(nextLevel);
                }
            }
            int totalCacheSize = levels.get(level.name()).totalCacheSize();
            int visibleCacheSize = levels.get(level.name()).visibleCacheSize();
            for( RetentionLevel l : pointingToCurrentLevel ){
                totalCacheSize = totalCacheSize + levels.get(l.name()).totalCacheSize();
                visibleCacheSize = visibleCacheSize + levels.get(l.name()).visibleCacheSize();
            }
            if(visibleCacheSize > totalCacheSize){
                throw new InvalidConfigurationException("visibleCacheSize > totalCacheSize for " + level.name());
            }
            levels.replace(level.name(), new CachingLevel( level.name(), visibleCacheSize, totalCacheSize, levels.get(level.name()).cacheLineWidth() ));
        }
        
    }

    
    public Optional<CachingLevel> findLevelForLevelName(String name){
        return Optional.ofNullable(levels.get(name));
    }
    
    public double qualityOfService(){
        return this.qualityOfService;
    }
    
    public double getPercentageOfMetricsToSave(){
        return this.percentageOfMetricsToSave;
    }

    public double getPercentageOfMetricsToTransfer(){
        return this.percentageOfMetricsToTransfer;
    }
    
    public String getMetricStorage(){
        return this.metricStorage;
    }
    
    public int getDatabaseThreads(){
        return this.numberOfDatabaseQueryThreads;
    }
    
    public int getMetricCacheThreads(){
        return this.numberOfWriteToCacheThreads;
    }
}
