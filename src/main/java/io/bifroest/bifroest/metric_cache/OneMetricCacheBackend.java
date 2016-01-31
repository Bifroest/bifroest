package com.goodgame.profiling.graphite_bifroest.metric_cache;

/**
 * Non-threadsafe backend for OneMetricCache, implemented as a sort-of
 * ringbuffer.
 *
 * Viewed from the outside, this class looks kind-of like an unlimited array of
 * doubles. You can put() doubles into the array and get() them again. But: As
 * you add values to higher indices, values at lower indices get dropped.
 *
 * @author sglimm
 */
class OneMetricCacheBackend {
    private final double values[];

    private int lowerBigBound;
    private int upperBigBound;

    public OneMetricCacheBackend( int size ) {
        values = new double[size];
        reset();
    }

    public void reset() {
        lowerBigBound = 0;
        upperBigBound = 0;
    }

    public int lowerBound() {
        return lowerBigBound;
    }

    public int upperBound() {
        return upperBigBound;
    }

    public int size() {
        return values.length;
    }

    private int smallIndex( int bigIndex ) {
        return bigIndex % values.length;
    }

    private void bumpBounds( int minimumNewBigUpperBound ) {
        if ( minimumNewBigUpperBound > upperBigBound ) {
            if ( minimumNewBigUpperBound - upperBigBound > values.length ) {
                // Performance optimization:
                // Don't go through timestamp(now) iterations on initial put().
                for ( int i = 0; i < values.length; i++ ) {
                    values[i] = Double.NaN;
                }
            } else {
                for ( int i = upperBigBound; i < minimumNewBigUpperBound - 1; i++ ) {
                    values[smallIndex( i )] = Double.NaN;
                }
            }
            upperBigBound = minimumNewBigUpperBound;
        }
        lowerBigBound = Math.max( lowerBigBound, upperBigBound - values.length );
    }

    public void put( int bigIndex, double value ) {
        if ( lowerBigBound > bigIndex )
            return;

        bumpBounds( bigIndex + 1 );

        values[smallIndex( bigIndex )] = value;
    }

    public double get( int bigIndex ) {
        if ( lowerBigBound <= bigIndex && bigIndex < upperBigBound ) {
            return values[smallIndex( bigIndex )];
        } else {
            return Double.NaN;
        }
    }
}
