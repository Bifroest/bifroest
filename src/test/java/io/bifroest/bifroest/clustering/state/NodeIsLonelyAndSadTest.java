package com.goodgame.profiling.graphite_bifroest.clustering.state;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;

import org.junit.Test;
import org.mockito.Mock;

import com.goodgame.profiling.bifroest.bifroest_client.metadata.ClusterState;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.bifroest.bifroest_client.seeds.HostPortPair;
import com.goodgame.profiling.bifroest.bifroest_client.seeds.KnownClusterStateRequester;

public final class NodeIsLonelyAndSadTest extends NodeStateTestFixture {
    @Mock private KnownClusterStateRequester requester;

    private NodeIsLonelyAndSad newSubject( HostPortPair... seeds ) {
        return new NodeIsLonelyAndSad( requester, Arrays.asList( seeds ), 
                metricCache );
    }
    
    @Test
    public void testThatTheNodeDoesntAnswerGetKnownNodes() {
        NodeIsLonelyAndSad subject = newSubject();
        assertThat( subject.doIAnswerGetClusterState(), is( false ));
    }

    @Test
    public void testThatTheNodeDoesntAnswerMetricRequests() {
        NodeIsLonelyAndSad subject = newSubject();
        assertThat( subject.doIAnswerGetMetrics(), is( false ));
        assertThat( subject.doIAnswerGetSubmetrics(), is( false ));
    }

    @Test
    public void testThatAnswersSetTheClusterStateAndStartJoining() throws IOException {
        // seed 1 isn't ready yet -- this might even be the node itself
        when( requester.request( HostPortPair.of( "seed1", 1000 ) ) )
            .thenReturn( Optional.empty() );

        ClusterState someReturnedClusterState = new ClusterState();
        NodeMetadata myOwnMetadata = NodeMetadata.forTest();
        NodeMetadata someExistingNode = NodeMetadata.forTest();
        Collection<NodeMetadata> alreadyKnownNodes = new ArrayList<>();
        alreadyKnownNodes.add( someExistingNode );
        someReturnedClusterState.addAll( alreadyKnownNodes );
        someReturnedClusterState.setLeader( someExistingNode );

        // seed 2 knows something
        when( requester.request( HostPortPair.of( "seed2", 2000 ) ) )
            .thenReturn( Optional.of( someReturnedClusterState ) );

        NodeIsLonelyAndSad subject = newSubject( HostPortPair.of( "seed1", 1000 ),
                                                 HostPortPair.of( "seed2", 2000 ));

        NodeState nextState = subject.startClustering( commands, clusterState, communication, myOwnMetadata );

        verify( communication ).connectAndIntroduceToAll( alreadyKnownNodes );
        assertThat( clusterState.getKnownNodes(), containsInAnyOrder( myOwnMetadata, someExistingNode ) );
        assertThat( clusterState.getLeader(), is( someExistingNode ) );
        assertThat( nextState, is( instanceOf( NodeIsJoining.class ) ) );
    }
}
