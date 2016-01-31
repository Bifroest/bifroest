package com.goodgame.profiling.graphite_bifroest.clustering.state;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;

public class NodeIsLeaderTest extends NodeStateTestFixture {
    private NodeIsLeader subject;

    @Before
    public void createSubject() {
        subject = new NodeIsLeader();
    }

    @Test
    public void testThatTheNodeDoAnswerGetKnownNodes() {
        assertThat( subject.doIAnswerGetClusterState(), is( true ));
    }

    @Test
    public void testThatTheNodeDoAnswerMetricRequests() {
        assertThat( subject.doIAnswerGetMetrics(), is( true ));
        assertThat( subject.doIAnswerGetSubmetrics(), is( true ));
    }

    @Test
    public void testThatTheNodeAddNewJoinedNodeToClusterState() throws IOException {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest();
        NodeMetadata newJoinedNodeMetadata = NodeMetadata.forTest();

        clusterState.setLeader( myOwnMetadata );
        clusterState.setBucketMapping( mapping );
        subject.reactToJoin(clusterState, myOwnMetadata, newJoinedNodeMetadata, communication, metricCache );

        assertTrue( clusterState.getKnownNodes().contains( newJoinedNodeMetadata ) );
        assertThat( subject, is( instanceOf( NodeIsLeader.class ) ) );
        assertThat( clusterState.getLeader(), is( myOwnMetadata ) );
    }

    @Test
    public void testThatTheNodeAltersAndBroadcastsTheBucketMappingForANewNode () throws IOException {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest();
        NodeMetadata newJoinedNodeMetadata = NodeMetadata.forTest();

        JSONObject serializedMapping = new JSONObject().put( "bukkit", "walrus" );

        when( mapping.toJSON() ).thenReturn( serializedMapping );

        clusterState.setLeader( myOwnMetadata );
        clusterState.setBucketMapping( mapping );
        subject.reactToJoin(clusterState, myOwnMetadata, newJoinedNodeMetadata, communication, metricCache );

        verify( mapping ).joinNode( newJoinedNodeMetadata );
        verify( communication ).broadcastNewBucketMapping();
    }

    @Test
    public void testThatTheNodeRemovesLeavingNodeFromClusterStateAndConnectionMap() throws IOException {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest();
        NodeMetadata leavingNodeMetadata = NodeMetadata.forTest();

        clusterState.addNode( myOwnMetadata );
        clusterState.addNode( leavingNodeMetadata );
        clusterState.setLeader( myOwnMetadata );
        clusterState.setBucketMapping( mapping );

        NodeState newState = subject.reactToLeave( clusterState, myOwnMetadata, leavingNodeMetadata, communication );

        assertTrue( clusterState.getKnownNodes().contains( myOwnMetadata ) );
        assertFalse( clusterState.getKnownNodes().contains( leavingNodeMetadata ) );
        assertThat( newState, is( instanceOf( NodeIsLeader.class ) ) );
        assertThat( clusterState.getLeader(), is( myOwnMetadata ) );
        verify( communication ).disconnectFrom( leavingNodeMetadata );
    }

    @Test
    public void testThatTheNodeAltersAndBroadcastsTheBucketMappingAfterLeavingNode () throws IOException {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest();
        NodeMetadata leavingNodeMetadata = NodeMetadata.forTest();
        JSONObject serializedMapping = new JSONObject().put( "bukkit", "walrus" );

        when( mapping.toJSON() ).thenReturn( serializedMapping );

        clusterState.addNode( myOwnMetadata );
        clusterState.addNode( leavingNodeMetadata );
        clusterState.setLeader( myOwnMetadata );
        clusterState.setBucketMapping( mapping );

        subject.reactToLeave(clusterState, myOwnMetadata, leavingNodeMetadata, communication );

        verify( mapping ).leaveNode( leavingNodeMetadata );
        verify( communication ).broadcastNewBucketMapping();
    }

    @Test
    public void testThatCallingReactToLeaveMultipleTimesOnlyRemovesOnce() throws IOException {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest();
        NodeMetadata leavingNodeMetadata = NodeMetadata.forTest();

        clusterState.addNode( myOwnMetadata );
        clusterState.addNode( leavingNodeMetadata );
        clusterState.setLeader( myOwnMetadata );
        clusterState.setBucketMapping( mapping );

        NodeState intermediateState = subject.reactToLeave( clusterState, myOwnMetadata, leavingNodeMetadata, communication );
        NodeState newState = intermediateState.reactToLeave( clusterState, myOwnMetadata, leavingNodeMetadata, communication );

        assertTrue( clusterState.getKnownNodes().contains( myOwnMetadata ) );
        assertFalse( clusterState.getKnownNodes().contains( leavingNodeMetadata ) );
        assertThat( newState, is( instanceOf( NodeIsLeader.class ) ) );
        assertThat( clusterState.getLeader(), is( myOwnMetadata ) );
        verify( communication, times(1) ).disconnectFrom( leavingNodeMetadata );
    }

    @Test
    public void testThatWhenSomeoneDesperatelyWantsToBeANewLeaderWeAllowHimTo() throws IOException {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest();
        NodeMetadata otherMetadata = NodeMetadata.forTest();

        clusterState.addNode( myOwnMetadata );
        clusterState.addNode( otherMetadata );
        clusterState.setLeader( myOwnMetadata );

        NodeState newState = subject.reactToNewLeader( clusterState, myOwnMetadata, otherMetadata );

        assertThat( clusterState.getLeader(), is( otherMetadata ) );
        assertThat( newState, is( instanceOf( NodeIsMember.class ) ) );
    }
}
