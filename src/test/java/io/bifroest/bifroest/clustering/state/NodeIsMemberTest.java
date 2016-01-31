package com.goodgame.profiling.graphite_bifroest.clustering.state;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.goodgame.profiling.bifroest.balancing.BucketMapping;
import com.goodgame.profiling.bifroest.balancing.KeepNeighboursMapping;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;

public class NodeIsMemberTest extends NodeStateTestFixture {
    private NodeIsMember subject;

    @Before
    public void createSubject() {
        subject = new NodeIsMember();
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

        NodeState newState = subject.reactToJoin(clusterState, myOwnMetadata, newJoinedNodeMetadata, communication, metricCache );

        assertThat( newState, is( instanceOf( NodeIsMember.class ) ) );
        assertTrue( clusterState.getKnownNodes().contains( newJoinedNodeMetadata ) );
    }

    @Test
    public void testThatTheNodeRemovesLeavingNodeFromClusterStateAndConnectionMap() throws IOException {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest( "abc", 1 );
        NodeMetadata leavingNodeMetadata = NodeMetadata.forTest( "def", 1 );
        NodeMetadata leaderMetadata = NodeMetadata.forTest( "ghi", 1 );

        clusterState.addNode( myOwnMetadata );
        clusterState.addNode( leavingNodeMetadata );
        clusterState.addNode( leaderMetadata );
        clusterState.setLeader( leaderMetadata );

        subject.reactToLeave( clusterState, myOwnMetadata, leavingNodeMetadata, communication );

        assertTrue( clusterState.getKnownNodes().contains( myOwnMetadata ) );
        assertFalse( clusterState.getKnownNodes().contains( leavingNodeMetadata ) );
        verify( communication ).disconnectFrom( leavingNodeMetadata );
    }

    @Test
    public void testThatTheNodeUpdatesItsMapping() throws IOException {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest();
        NodeMetadata anotherNodeMetadata = NodeMetadata.forTest();
        clusterState.setBucketMapping( mapping );

        BucketMapping<NodeMetadata> newMapping = new KeepNeighboursMapping<>( myOwnMetadata );
        newMapping.joinNode( anotherNodeMetadata );

        subject.reactToNewMapping( clusterState, myOwnMetadata, metricCache, newMapping, communication );

        assertThat( clusterState.getBucketMapping(), is( newMapping ) );
    }

    @Test
    public void testThatReactingToNewLeaderUpdatesOurKnownLeader() throws IOException {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest();
        NodeMetadata newLeaderMetadata = NodeMetadata.forTest();

        clusterState.setLeader( myOwnMetadata );

        subject.reactToNewLeader( clusterState, myOwnMetadata, newLeaderMetadata );

        assertThat( clusterState.getLeader(), is( newLeaderMetadata ) );
    }

    @Test
    public void testThatALeavingLeaderCausesTheNewLeaderToAssumeLeadership() throws IOException {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest( "abc", 1 );
        NodeMetadata oldLeaderMetadata = NodeMetadata.forTest( "def", 1 );
        NodeMetadata someOtherNodeMetadata = NodeMetadata.forTest( "ghi", 1 );

        clusterState.addNode( myOwnMetadata );
        clusterState.addNode( oldLeaderMetadata );
        clusterState.addNode( someOtherNodeMetadata );
        clusterState.setLeader( oldLeaderMetadata );
        clusterState.setBucketMapping( mapping );

        NodeState newState = subject.reactToLeave( clusterState, myOwnMetadata, oldLeaderMetadata, communication );

        assertThat( clusterState.getLeader(), is( myOwnMetadata ) );
        assertThat( newState, is( instanceOf( NodeIsLeader.class ) ) );
        verify( communication ).broadcastNewLeader( myOwnMetadata );
        verify( communication ).broadcastNewBucketMapping();
        verify( mapping ).leaveNode( oldLeaderMetadata );
    }

    @Test
    public void testThatWhenSomeoneElseLeavesLeadershipStaysTheSame() throws IOException {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest( "abc", 1 );
        NodeMetadata oldLeaderMetadata = NodeMetadata.forTest( "def", 1 );
        NodeMetadata someOtherNodeMetadata = NodeMetadata.forTest( "ghi", 1 );

        clusterState.addNode( myOwnMetadata );
        clusterState.addNode( oldLeaderMetadata );
        clusterState.addNode( someOtherNodeMetadata );
        clusterState.setLeader( oldLeaderMetadata );

        NodeState newState = subject.reactToLeave( clusterState, myOwnMetadata, someOtherNodeMetadata, communication );

        assertThat( clusterState.getLeader(), is( oldLeaderMetadata ) );
        assertThat( newState, is( instanceOf( NodeIsMember.class ) ) );
        verify( communication, never() ).broadcastNewLeader( any() );
    }
}
