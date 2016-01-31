package com.goodgame.profiling.graphite_bifroest.clustering.communication;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.goodgame.profiling.bifroest.balancing.BucketMapping;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.ClusterState;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.MappingFactory;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;
import com.goodgame.profiling.commons.systems.cron.TaskRunner;
import com.goodgame.profiling.commons.systems.cron.TaskRunner.TaskID;
import com.goodgame.profiling.graphite_bifroest.clustering.ClusteringCommandInterface;
import com.goodgame.profiling.graphite_bifroest.clustering.statistics.AbandonedMetricsReceivedEvent;
import com.goodgame.profiling.graphite_bifroest.clustering.statistics.TransferredMetricsReceivedEvent;

public final class BifroestRemote {
    private static final Logger log = LogManager.getLogger();

    private final ClusteringCommandInterface clusteringCommands;
    private final ClusterState clusterState;

    private final NodeMetadata myOwnMetadata;
    private final NodeMetadata remoteMetadata;

    private final BlockingQueue<String> outputQueue;
    private final OutputThread outputThread;
    private final ReaderThread readerThread;

    private final TaskID pingTask;
    private long sequenceIdSent;
    private long sequenceIdAcked;

    public BifroestRemote( ClusteringCommandInterface clusteringCommands, ClusterState clusterState, Socket socket, NodeMetadata myOwnMetadata, NodeMetadata newNode, Duration pingFrequency ) throws IOException {
        this.clusteringCommands = clusteringCommands;
        this.clusterState = clusterState;

        this.myOwnMetadata = myOwnMetadata;
        this.remoteMetadata = newNode;

        this.outputQueue = new LinkedBlockingQueue<>();
        this.outputThread = new OutputThread( socket );
        this.outputThread.start();

        this.readerThread = new ReaderThread( newNode, socket );
        this.readerThread.start();

        this.sequenceIdSent = 0;
        this.sequenceIdAcked = 0;
        this.pingTask = startPinging( pingFrequency );
    }

    private TaskID startPinging( Duration pingFrequency ) {
        return TaskRunner.runRepeated(
                () -> {
                    if ( sequenceIdAcked != sequenceIdSent ) {
                        log.warn( "Last ping wasn't acknowledged for remote {}. Connection is stale, disconnecting!", remoteMetadata );
                        log.warn( "sequenceIdAcked: {}, sequenceIdSent: {}", sequenceIdAcked, sequenceIdSent );
                        clusteringCommands.reactToLeaveSoon( remoteMetadata );
                    }
                    log.debug( "Sending ping to {}", remoteMetadata );
                    sendMessageSoon( new JSONObject()
                            .put( "command", "ping" )
                            .put( "sequence-id", ++sequenceIdSent ) );
                },
                "ping-" + remoteMetadata.getFunnyNameForDebugging(),
                pingFrequency, pingFrequency,
                true );
    }

    public void disconnect() {
        TaskRunner.stopTask( pingTask );

        readerThread.dontRunAnymore();

        outputThread.dontRunAnymore();
    }

    public void sendMessageSoon( JSONObject message ) {
        try {
            synchronized ( outputThread.shutdownLock ) {
                if ( !outputThread.isRunning() ) {
                    throw new IllegalStateException( "Output thread was already shut down!" );
                }
                outputQueue.put( message.toString() + "\n" );
            }
        } catch ( InterruptedException e ) {
            log.warn( "Unexpectedly interrupted!", e );
        }
    }

    public void onMessage( JSONObject message ) {
        switch ( message.getString( "command" ) ) {
        case "ping":
            onPing( message );
            break;
        case "pong":
            onPong( message );
            break;
        case "update-mapping":
            onUpdateMapping( message );
            break;
        case "transfer-metrics":
            onTransferMetrics( message );
            break;
        case "new-leader":
            onNewLeader( message );
            break;
        case "i-am-leaving":
            onLeavingNode( message );
            break;
        default:
            log.warn( "Cannot handle command {}", message.getString( "command" ) );
        }
    }

    private void onPing( JSONObject message ) {
        log.debug( "Sending pong" );
        sendMessageSoon( new JSONObject()
                .put( "command", "pong" )
                .put( "sequence-id", message.getLong( "sequence-id" ) ) );
    }

    private void onPong( JSONObject message ) {
        long got = message.getLong( "sequence-id" );
        long expected = sequenceIdSent;
        if ( got != expected ) {
            log.warn( "Got wrong sequence ID from {} - got {}, expected {}", remoteMetadata, got, expected );
        }
        sequenceIdAcked = got;
    }

    private void onUpdateMapping( JSONObject message ) {
        BucketMapping<NodeMetadata> newMapping = MappingFactory.fromJSON( message.getJSONObject( "mapping" ) );
        clusteringCommands.reactToNewMappingSoon( newMapping );
    }

    private void onNewLeader( JSONObject message ) {
        NodeMetadata newLeader = NodeMetadata.fromJSON( message.getJSONObject( "new-leader" ) );
        clusteringCommands.reactToNewLeaderSoon( newLeader );
    }

    private void onTransferMetrics( JSONObject message ) {
        List<String> metrics = new ArrayList<>();
        JSONArray jsonMetrics = message.getJSONArray( "metrics" );
        for ( int i = 0; i < jsonMetrics.length(); i++ ) {
            metrics.add( jsonMetrics.getString( i ) );
        }
        TransferredMetricsReceivedEvent.fire( metrics );
        clusteringCommands.reactToTransferredMetricsSoon( metrics );
    }

    private void onLeavingNode( JSONObject message ) {
        NodeMetadata leavingNode = NodeMetadata.fromJSON( message.getJSONObject( "leaving-node" ) );
        if ( clusterState.getLeader().equals( myOwnMetadata ) ) {
            List<String> metrics = new ArrayList<>();
            JSONArray jsonMetrics = message.getJSONArray( "abandoned-metrics" );
            for ( int i = 0; i < jsonMetrics.length(); i++ ) {
                metrics.add( jsonMetrics.getString( i ) );
            }
            AbandonedMetricsReceivedEvent.fire( metrics );
            clusteringCommands.reactToAbandonedMetricsSoon( leavingNode, metrics );
        } else {
            clusteringCommands.reactToLeaveSoon( leavingNode );
        }
    }

    private final class ReaderThread extends Thread {
        private final NodeMetadata source;
        private final Socket socket;
        private final BufferedReader fromNode;
        private volatile boolean running;

        public ReaderThread( NodeMetadata source, Socket socket ) throws IOException {
            this.source = source;
            this.socket = socket;
            this.fromNode = new BufferedReader( new InputStreamReader( socket.getInputStream() ) );
            this.running = true;
        }

        public void dontRunAnymore() {
            running = false;
            try {
                socket.close();
                fromNode.close();
            } catch ( IOException e ) {
                log.warn( "Exception while closing socket", e );
            }
        }

        @Override
        public void run() {
            while ( running ) {
                try {
                    log.debug( "Hello world from reader thread" );
                    String input = fromNode.readLine();
                    log.debug( "Incoming from {}: {}", source, input );
                    if ( input == null ) {
                        log.debug( "Terminating Reader thread for {}", source );
                        running = false;
                        clusteringCommands.reactToLeaveSoon( source );
                        return;
                    }
                    JSONObject message = new JSONObject( input );
                    onMessage( message );
                } catch ( IOException e ) {
                    log.info( "Couldn't receive message - connection to {} lost: {}", source, e.getMessage() );
                    clusteringCommands.reactToLeaveSoon( source );
                    return;
                } catch ( Exception e ) {
                    log.warn( "Unexpected exception in reader thread!", e );
                }
            }
        }
    }

    private final class OutputThread extends Thread {
        private volatile boolean running;

        private final Socket socket;
        private final Writer toNode;

        public final Object shutdownLock = new Object();

        public OutputThread( Socket socket ) throws IOException {
            this.socket = socket;
            this.toNode = new BufferedWriter( new OutputStreamWriter( socket.getOutputStream() ) );
            this.running = true;
        }

        public boolean isRunning() {
            return running;
        }

        public void dontRunAnymore() {
            synchronized ( shutdownLock ) {
                running = false;
            }
            interrupt();
            try {
                join();
            } catch ( InterruptedException e ) {
                log.warn( "Exception while waiting for OutputThread!", e );
            }

            while ( !outputQueue.isEmpty() ) {
                sendNextMessageFromQueue();
            }

            try {
                socket.close();
                toNode.close();
            } catch ( IOException e ) {
                log.warn( "Exception while disconnecting {}: {}", remoteMetadata, e.getMessage() );
            }
       }

        @Override
        public void run() {
            while ( running ) {
                sendNextMessageFromQueue();
            }
        }

        private void sendNextMessageFromQueue() {
            try {
                String message = outputQueue.take();

                log.debug( "Sending to {}: {}", remoteMetadata, message );

                toNode.write(message);
                toNode.flush();
            } catch ( IOException e ) {
                log.info( "Couldn't send message - connection to {} lost: {}", remoteMetadata, e.getMessage());
                clusteringCommands.reactToLeaveSoon( remoteMetadata );
            } catch ( InterruptedException e ) {
                log.info("OutputThread was interrupted - this is expected!");
            } catch ( Exception e ) {
                log.warn("Unexpected exception in reader thread!", e);
            }
        }
    }
}