package com.goodgame.profiling.graphite_bifroest.clustering.communication;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.Factory;
import org.apache.commons.collections4.map.LazyMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.goodgame.profiling.bifroest.balancing.BucketMapping;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.ClusterState;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.graphite_bifroest.clustering.ClusteringCommandInterface;
import com.goodgame.profiling.graphite_bifroest.clustering.statistics.AbandonedMetricsSentEvent;
import com.goodgame.profiling.graphite_bifroest.clustering.statistics.TransferredMetricsSentEvent;

public final class SocketBasedClusterCommunication implements MutableClusterCommunication, ClientCommunication {
    private static final Logger log = LogManager.getLogger();

    private final ServerThread clusterServer;
    private final Map<NodeMetadata, BifroestRemote> remotes;

    private final List<ClientRemote> clients;

    private final ClusteringCommandInterface clusteringCommand;
    private final ClusterState clusterState;
    private final NodeMetadata myOwnMetadata;

    private final Duration pingFrequency;

    private final Object theLock = new Object();

    public SocketBasedClusterCommunication(
            ClusteringCommandInterface clusteringCommand, ClusterState clusterState,
            NodeMetadata myOwnMetadata,
            Duration pingFrequency ) throws IOException {
        if ( !clusterState.getKnownNodes().isEmpty() ) {
            throw new IllegalStateException( "Why am I being created with an existing cluster?!" );
        }

        ServerSocket serverSocket = new ServerSocket( myOwnMetadata.getPorts().getClusterPort() );
        serverSocket.setReuseAddress( true );

        this.clusterServer = new ServerThread( serverSocket );
        this.clusterServer.start();

        this.remotes = new HashMap<>();
        this.clients = new ArrayList<>();
        
        this.clusteringCommand = clusteringCommand;
        this.clusterState = clusterState;
        this.myOwnMetadata = myOwnMetadata;

        this.pingFrequency = pingFrequency;
    }

    @Override
    public void sendMetricsToNodes( Collection<String> metricsToSend ) {
        TransferredMetricsSentEvent.fire( metricsToSend );
        synchronized ( theLock ) {
            Map<NodeMetadata, JSONArray> metricsToTransfer = LazyMap.lazyMap( new HashMap<NodeMetadata, JSONArray>(), (Factory<JSONArray>) JSONArray::new );

            for ( String metricName : metricsToSend ) {
                int metricHash = metricName.hashCode();
                NodeMetadata targetNode = clusterState.getBucketMapping().getNodeFor( metricHash );
                metricsToTransfer.get( targetNode ).put( metricName );
            }

            JSONObject message = new JSONObject();
            message.put( "command", "transfer-metrics" );

            for ( Map.Entry<NodeMetadata, JSONArray> entry : metricsToTransfer.entrySet() ) {
                message.put( "metrics", entry.getValue() );
                sendToNode( entry.getKey(), message );
            }
        }
    }

    @Override
    public void informLeaderAboutLeavingAndAbandonedMetrics( NodeMetadata leavingNodeMetadata, Collection<String> abandonedMetrics ) {
        synchronized ( theLock ) {
            JSONArray metrics = new JSONArray();
            abandonedMetrics.forEach( metric -> metrics.put( metric ) );

            JSONObject message = new JSONObject();
            message.put( "command", "i-am-leaving" );
            message.put( "leaving-node", leavingNodeMetadata.toJSON() );
            message.put( "abandoned-metrics", metrics );

            AbandonedMetricsSentEvent.fire( abandonedMetrics );
            sendToLeader( message );
        }
    }

    private void sendToNode( NodeMetadata target, JSONObject message ) {
        remotes.get( target ).sendMessageSoon( message );
    }

    @Override
    public void sendToLeader( JSONObject message ) {
        synchronized ( theLock ) {
            sendToNode( clusterState.getLeader(), message );
        }
    }

    @Override
    public void broadcastNewBucketMapping() {
        synchronized ( theLock ) {
            BucketMapping<NodeMetadata> mapping = clusterState.getBucketMapping();
            JSONObject message = new JSONObject();
            message.put( "command" , "update-mapping" );
            message.put( "mapping", mapping.toJSON() );
            sendToAll( message );
            broadcastNewMappingToClients( mapping );
        }
    }

    @Override
    public void broadcastNewLeader( NodeMetadata newLeader ) {
        synchronized ( theLock ) {
            JSONObject message = new JSONObject();
            message.put( "command", "new-leader" );
            message.put( "new-leader", newLeader.toJSON() );
            sendToAll( message );
        }
    }

    private void sendToAll( JSONObject messageForEveryone ) {
        for ( NodeMetadata node : remotes.keySet() ) {
            sendToNode( node, messageForEveryone );
        }
    }

    @Override
    public void connectAndIntroduceTo( NodeMetadata newNode ) throws IOException {
        synchronized ( theLock ) {
            log.debug( "Creating connection to {}", newNode );
            Socket toNewNode = new Socket( newNode.getAddress(), newNode.getPorts().getClusterPort() );
            BifroestRemote remote = new BifroestRemote( clusteringCommand, clusterState, toNewNode, myOwnMetadata, newNode, pingFrequency );
            remotes.put( newNode, remote );
            remote.sendMessageSoon( new JSONObject()
                    .put( "command", "hello" )
                    .put( "who-am-i", myOwnMetadata.toJSON() ) );
        }
    }

    @Override
    public void disconnectFrom( NodeMetadata leavingNode ) throws IOException {
        synchronized ( theLock ) {
            if ( remotes.containsKey( leavingNode ) ) {
                remotes.get( leavingNode ).disconnect();
                remotes.remove( leavingNode );
            }
        }
    }

    @Override
    public void shutdown() {
        synchronized ( theLock ) {
            clusterServer.dontRunAnymore();
            clusterServer.interrupt();

            for ( BifroestRemote remote : remotes.values() ) {
                remote.disconnect();
            }
        }
    }

    @Override
    public void disconnectClient( ClientRemote who ) {
        synchronized ( theLock ) {
            if ( clients.contains( who ) ) {
                clients.remove( who );
                who.disconnect();
            }
        }
    }

    @Override
    public void broadcastNewNodeToClients( NodeMetadata newNodeMetadata ) {
        synchronized ( theLock ) {
            clients.forEach( client -> client.sendNewNode( newNodeMetadata ) );
        }
    }

    @Override
    public void broadcastNewMappingToClients( BucketMapping<NodeMetadata> newMapping ) {
        synchronized ( theLock ) {
            clients.forEach( client -> client.sendBucketMapping( newMapping ) );
        }
    }

    private final class ServerThread extends Thread {
        private final ServerSocket server;
        private volatile boolean running;

        public ServerThread( ServerSocket server ) {
            this.server = server;
            this.running = true;
        }

        public void dontRunAnymore() {
            running = false;
        }

        @Override
        public void run() {
            try {
                while ( running ) {
                    Socket client = this.server.accept();
                    BufferedReader fromClient = new BufferedReader( new InputStreamReader( client.getInputStream() ) );
                    String firstCommandUnparsed = fromClient.readLine();
                    log.debug( "Incoming new connection from {}: {}", client.getRemoteSocketAddress(), firstCommandUnparsed );
                    JSONObject firstCommand = new JSONObject( firstCommandUnparsed );
                    String command = firstCommand.getString( "command" );

                    if ( command.equals( "hello" ) ) {
                        synchronized ( theLock ) {
                            NodeMetadata newNodeMetadata = NodeMetadata.fromJSON( firstCommand.getJSONObject( "who-am-i" ) );
                            BifroestRemote remote = new BifroestRemote( clusteringCommand, clusterState, client, myOwnMetadata, newNodeMetadata, pingFrequency );
                            remotes.put( newNodeMetadata, remote );
                            clusteringCommand.reactToJoinSoon( newNodeMetadata );
                            if ( clusterState.getLeader().equals( myOwnMetadata ) ) {
                                broadcastNewNodeToClients( newNodeMetadata );
                            }
                        }
                    } else if ( command.equals( "hello-i-am-a-client" ) ) {
                        synchronized ( theLock ) {
                            ClientRemote newClient = new ClientRemote( client, SocketBasedClusterCommunication.this );
                            clients.add( newClient );
                            newClient.sendBucketMapping( clusterState.getBucketMapping() );
                        }
                    } else {
                        log.info( "Cannot understand request {}", firstCommandUnparsed );
                        client.close();
                    }
                }
            } catch ( IOException e ) {
                // TODO: handle io exceptions in better ways
                log.warn( "Waaah IO error", e );
            }

        }
    }
}
