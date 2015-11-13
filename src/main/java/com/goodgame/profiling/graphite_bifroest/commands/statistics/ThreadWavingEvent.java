package com.goodgame.profiling.graphite_bifroest.commands.statistics;

import java.time.Clock;
import java.time.Instant;

import com.goodgame.profiling.commons.statistics.commands.EventWithThreadId;
import com.goodgame.profiling.commons.statistics.EventWithInstant;

public abstract class ThreadWavingEvent implements EventWithThreadId,EventWithInstant {
    private final Instant when;
    private final long threadId;

    public ThreadWavingEvent( Clock clock ) {
        this( clock, Thread.currentThread().getId() );
    }

    public ThreadWavingEvent( Clock clock, long threadId ) {
        when = clock.instant();
        this.threadId = threadId;
    }

    @Override
    public Instant when() {
        return when;
    }

    @Override
    public long threadId() {
        return threadId;
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals( Object o ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return String.format( "%s(when=%s,threadId=%d)", getClass().getSimpleName(), when, threadId );
    }
}

