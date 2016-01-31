package io.bifroest.bifroest.metric_cache;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Deque;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class BackendPool {
    private static final Logger log = LogManager.getLogger();

    private final Deque<OneMetricCacheBackend> freeBackends;
    private final ReferenceQueue<OneMetricCache> refQueue;
    private final Set<Reference<OneMetricCache>> references;

    private int totalCacheSize;
    private int toRemove;
    private int minimumRemaining;

    private final BackendPoolTracker tracker;

    private final Object lock = new Object();

    private final Thread reclaimThread;

    private volatile boolean shutting_down;

    public BackendPool( String cacheName, int visibleCacheSize, int totalCacheSize, int cacheLineWidth ) {
        this.tracker = BackendPoolTracker.storingIn( "Caches", cacheName );

        this.totalCacheSize = totalCacheSize;
        this.toRemove = 0;
        this.minimumRemaining = totalCacheSize - visibleCacheSize;

        this.freeBackends = new ConcurrentLinkedDeque<>();
        for ( int i = 0; i < totalCacheSize; i++ ) {
            this.freeBackends.addLast( new OneMetricCacheBackend( cacheLineWidth ) );
            tracker.backendCreated( totalCacheSize, toRemove, minimumRemaining, i + 1 );
        }

        this.refQueue = new ReferenceQueue<>();
        this.references = new HashSet<>();

        reclaimThread = new Thread( this::reclaimElementsFromRefQueue, "refQueueReclaimer " + cacheName );
        reclaimThread.start();
    }

    /*
     * Invariant:
     * totalCacheSize + toRemove == number of existing, active OneMetricCacheBackends
     */

    public int resize( int newVisibleCacheSize, int newTotalCacheSize, int cacheLineWidth ) {
        synchronized( lock ) {
            minimumRemaining = newTotalCacheSize - newVisibleCacheSize;

            while ( totalCacheSize < newTotalCacheSize ) {
                if ( toRemove > 0 ) {
                    toRemove--;
                    tracker.backendRemoved( totalCacheSize + 1, toRemove, minimumRemaining, freeBackends.size() );
                }
                else {
                    this.freeBackends.addLast( new OneMetricCacheBackend( cacheLineWidth ) );
                    tracker.backendCreated( totalCacheSize + 1, toRemove, minimumRemaining, freeBackends.size() );
                }
                totalCacheSize++;
            }

            while ( totalCacheSize > newTotalCacheSize && !this.isEmpty() && !freeBackends.isEmpty() ) {
                freeBackends.pollFirst();
                tracker.backendRemoved( totalCacheSize - 1, toRemove, minimumRemaining, freeBackends.size() );
                totalCacheSize--;
            }
            int numElementsToEvict = totalCacheSize - newTotalCacheSize;
            toRemove += numElementsToEvict;
            totalCacheSize = newTotalCacheSize;
            tracker.totalCacheSizeChanged( totalCacheSize, toRemove, minimumRemaining, freeBackends.size() );
            return numElementsToEvict;
        }
    }

    private void reclaimElementsFromRefQueue() {
        Reference<? extends OneMetricCache> current;
        try {
            while ( !shutting_down && ( current = refQueue.remove() ) != null ) {
                if ( current instanceof BackendPool.FrontendWeakReference ) {
                    synchronized( lock ) {
                        OneMetricCacheBackend backend = ( (FrontendWeakReference)current ).backend();
                        if ( toRemove == 0 ) {
                            backend.reset();
                            freeBackends.addFirst( backend );
                            tracker.backendReclaimed( totalCacheSize, toRemove, minimumRemaining, freeBackends.size() );
                        } else {
                            // This is virtually a remove/free, because we do not add ^
                            toRemove--;
                            tracker.backendRemoved( totalCacheSize, toRemove, minimumRemaining, freeBackends.size() );
                        }
                    }
                } else {
                    log.error( "Reference Queue contained something that wasn't a FrontendWeakReference!" );
                }
            }
        } catch( InterruptedException e ) {
            if ( shutting_down ) {
                log.info( "reclaimThread interrupted - this is expected" );
            } else {
                log.warn( "reclaimThread UNEXPECTEDLY interrupted", e );
            }
        }
    }

    private void cleanUpReferences() {
        references.removeIf( ref -> ref.get() == null );
    }

    public boolean isEmpty() {
        synchronized( lock ) {
            return freeBackends.size() <= minimumRemaining;
        }
    }

    public Optional<OneMetricCacheBackend> getNextFree() {
        synchronized( lock ) {
            cleanUpReferences();
            Optional<OneMetricCacheBackend> nextFree = Optional.ofNullable( freeBackends.pollFirst() );
            if ( nextFree.isPresent() ) {
                tracker.backendClaimed( totalCacheSize, toRemove, minimumRemaining, freeBackends.size() );
            }
            return nextFree;
        }
    }

    public void ungetBackend( OneMetricCacheBackend backend ) {
        synchronized( lock ) {
            freeBackends.add( backend );
            tracker.backendReclaimed( totalCacheSize, toRemove, minimumRemaining, freeBackends.size() );
        }
    }

    public void notifyFrontendCreated( OneMetricCache frontend ) {
        synchronized( lock ) {
            references.add( new FrontendWeakReference( frontend, refQueue ) );
        }
    }

    public void shutdown() {
        shutting_down = true;
        reclaimThread.interrupt();
    }

    private class FrontendWeakReference extends WeakReference<OneMetricCache> {
        private final OneMetricCacheBackend backend;

        private FrontendWeakReference( OneMetricCache frontend, ReferenceQueue<? super OneMetricCache> q ) {
            super( frontend, q );
            this.backend = frontend.getBackend();
        }

        public OneMetricCacheBackend backend() {
            return backend;
        }
    }
}
