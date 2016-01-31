package io.bifroest.bifroest.clustering.statistics;

import io.bifroest.commons.statistics.eventbus.EventBusManager;
import io.bifroest.bifroest.clustering.state.NodeState;

public final class NodeStateChangedEvent {
    private final String newState;

    private NodeStateChangedEvent( String newState ) {
        this.newState = newState;
    }

    public String getNewState() {
        return newState;
    }

    public static void fire( NodeState newState ) {
        EventBusManager.fire( new NodeStateChangedEvent( newState.getClass().getSimpleName() ) );
    }

    @Override
    public String toString() {
        return "NodeStateChangedEvent [newState=" + newState + "]";
    }
}
