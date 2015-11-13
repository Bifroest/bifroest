package com.goodgame.profiling.graphite_bifroest.metric_cache;

import java.util.Objects;

public final class CachingLevel {

    private final String name;
    private final int visibleCacheSize;
    private final int totalCacheSize;
    private final int cacheLineWidth;

    public CachingLevel( String name, int visibleCacheSize, int totalCacheSize, int cacheLineWidth ) {
        this.name = name;
        this.visibleCacheSize = visibleCacheSize;
        this.totalCacheSize = totalCacheSize;
        this.cacheLineWidth = cacheLineWidth;
    }

    public String name() {
        return name;
    }

    public int visibleCacheSize() {
        return visibleCacheSize;
    }

    public int totalCacheSize() {
        return totalCacheSize;
    }

    public int cacheLineWidth() {
        return cacheLineWidth;
    }

    @Override
    public int hashCode() {
        return Objects.hash( name, visibleCacheSize, totalCacheSize, cacheLineWidth );
    }

    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        } else if ( obj == null ) {
            return false;
        } else if ( !( obj instanceof CachingLevel ) ) {
            return false;
        }
        CachingLevel level = (CachingLevel)obj;
        boolean result = true;
        result &= name.equals( level.name );
        result &= visibleCacheSize == level.visibleCacheSize;
        result &= totalCacheSize == level.totalCacheSize;
        result &= cacheLineWidth == level.cacheLineWidth;
        return result;
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder( "CachingStrategy [" );
        result.append( "visibleCacheSize= " ).append( visibleCacheSize ).append( ", " );
        result.append( "totalCacheSize=" ).append( totalCacheSize ).append( ", " );
        result.append( "cacheLineWidth=" ).append( cacheLineWidth ).append( ", " );
        result.append( "]" );
        return result.toString();
    }
}
