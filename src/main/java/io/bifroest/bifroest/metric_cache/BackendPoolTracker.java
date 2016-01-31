package com.goodgame.profiling.graphite_bifroest.metric_cache;

import java.util.concurrent.atomic.LongAdder;

import com.goodgame.profiling.commons.statistics.WriteToStorageEvent;
import com.goodgame.profiling.commons.statistics.eventbus.EventBusManager;
import com.goodgame.profiling.commons.statistics.storage.MetricStorage;

public class BackendPoolTracker {
    private LongAdder created = new LongAdder();
    private LongAdder removed = new LongAdder();
    private LongAdder reclaimed = new LongAdder();
    private LongAdder claimed = new LongAdder();

    private volatile int totalsize;
    private volatile int toRemove;
    private volatile int minimumRemaining;
    private volatile int freeBackends;

    private BackendPoolTracker() {
    }

    public void backendCreated( int totalsize, int toRemove, int minimumRemaining,
            int freeBackends ) {
        created.increment();
        this.totalsize = totalsize;
        this.toRemove = toRemove;
        this.minimumRemaining = minimumRemaining;
        this.freeBackends = freeBackends;
    }

    public void backendRemoved( int totalsize, int toRemove, int minimumRemaining,
            int freeBackends ) {
        removed.increment();
        this.totalsize = totalsize;
        this.toRemove = toRemove;
        this.minimumRemaining = minimumRemaining;
        this.freeBackends = freeBackends;
    }

    public void backendReclaimed( int totalsize, int toRemove, int minimumRemaining,
            int freeBackends ) {
        reclaimed.increment();
        this.totalsize = totalsize;
        this.toRemove = toRemove;
        this.minimumRemaining = minimumRemaining;
        this.freeBackends = freeBackends;
    }

    public void backendClaimed( int totalsize, int toRemove, int minimumRemaining,
            int freeBackends ) {
        claimed.increment();
        this.totalsize = totalsize;
        this.toRemove = toRemove;
        this.minimumRemaining = minimumRemaining;
        this.freeBackends = freeBackends;
    }

    public void totalCacheSizeChanged( int totalsize, int toRemove, int minimumRemaining, int freeBackends ) {
        this.totalsize = totalsize;
        this.toRemove = toRemove;
        this.minimumRemaining = minimumRemaining;
        this.freeBackends = freeBackends;
    }

    private void writeTo( MetricStorage storage, String[] nameParts ) {
        for ( String np : nameParts )
            storage = storage.getSubStorageCalled( np );
        storage.store( "TotalSize", totalsize );
        storage.store( "BackendsToRemove", toRemove );
        storage.store( "MinimumRemaining", minimumRemaining );
        storage.store( "FreeBackends", freeBackends );
        storage.store( "BackendsCreated", created.doubleValue() );
        storage.store( "BackendsRemoved", removed.doubleValue() );
        storage.store( "BackendsReclaimed", reclaimed.doubleValue() );
        storage.store( "BackendsClaimed", claimed.doubleValue() );
    }

    public static BackendPoolTracker storingIn( String... nameParts ) {
        BackendPoolTracker r = new BackendPoolTracker();
        EventBusManager.createRegistrationPoint().sub( WriteToStorageEvent.class, e -> {
            r.writeTo( e.storageToWriteTo(), nameParts );
        } );
        return r;
    }

}
