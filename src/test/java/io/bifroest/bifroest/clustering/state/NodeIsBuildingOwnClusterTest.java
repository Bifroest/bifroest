package io.bifroest.bifroest.clustering.state;

import io.bifroest.balancing.BucketMapping;
import io.bifroest.bifroest_client.metadata.NodeMetadata;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Before;

public final class NodeIsBuildingOwnClusterTest extends NodeStateTestFixture {
    private NodeIsBuildingOwnCluster subject;

    @Before
    public void createSubject() {        
        subject = new NodeIsBuildingOwnCluster(metricCache);
    }

    @Test
    public void testThatTheNodeDoesntAnswerGetKnownNodes() {
        assertThat( subject.doIAnswerGetClusterState(), is( false ));
    }

    @Test
    public void testThatTheNodeDoesntAnswerMetricRequests() {
        assertThat( subject.doIAnswerGetMetrics(), is( false ));
        assertThat( subject.doIAnswerGetSubmetrics(), is( false ));
    }

    @Test
    public void testThatTheNodeSetItselfToLeader() {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest();
        NodeState newState = subject.buildANewCluster( clusterState, myOwnMetadata );

        assertThat( newState, is( instanceOf( NodeIsLeader.class) ) );
        assertThat( clusterState.getLeader(), is( myOwnMetadata ) );
    }

    @Test
    public void testThatNodeSetInitialBucketMapping() {
        NodeMetadata myOwnMetadata = NodeMetadata.forTest();
        subject.buildANewCluster( clusterState, myOwnMetadata );

        BucketMapping<NodeMetadata> nodeMapping = clusterState.getBucketMapping();
        assertThat( nodeMapping.getNodeFor( 42 ), is( myOwnMetadata ) );
    }
}
