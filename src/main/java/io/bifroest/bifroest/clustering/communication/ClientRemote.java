package com.goodgame.profiling.graphite_bifroest.clustering.communication;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.net.SocketAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.goodgame.profiling.bifroest.balancing.BucketMapping;
import com.goodgame.profiling.bifroest.bifroest_client.metadata.NodeMetadata;

public class ClientRemote {
    private static final Logger log = LogManager.getLogger();

    private final SocketAddress remoteAddress;
    private final Writer toClient;
    private final ReaderThread readerThread;
    
    private final ClientCommunication clientCommunication;

    public ClientRemote( Socket socket, ClientCommunication clientCommunication ) throws IOException {
        this.remoteAddress = socket.getRemoteSocketAddress();
        this.clientCommunication = clientCommunication;
        
        this.toClient = new BufferedWriter( new OutputStreamWriter( socket.getOutputStream() ) );
        this.readerThread = new ReaderThread( new BufferedReader( new InputStreamReader( socket.getInputStream() ) ) );
        this.readerThread.start();
    }

    public void sendBucketMapping( BucketMapping<NodeMetadata> mapping ) {
        JSONObject message = new JSONObject();
        message.put( "command" , "update-mapping" );
        message.put( "mapping", mapping.toJSON() );
        this.sendMessage( message );
    }

    public void sendNewNode( NodeMetadata newNodeMetadata ) {
        JSONObject message = new JSONObject();
        message.put( "command", "new-node" );
        message.put( "new-node", newNodeMetadata.toJSON() );
        this.sendMessage( message );
    }

    private void sendMessage( JSONObject message ) {
        log.debug( "Sending {} to {}", remoteAddress, message );
        try {
            toClient.write( message.toString() + "\n" );
            toClient.flush();
        } catch ( IOException e ) {
            log.info( "Couldn't send message - connection to {} lost: {}", remoteAddress, e.getMessage() );
            disconnect();
        }
    }

    public void disconnect() {
        clientCommunication.disconnectClient( this );
        
        readerThread.dontRunAnymore();
        readerThread.interrupt();

        try {
            toClient.close();
        } catch ( IOException e ) {
            log.warn( "Exception while disconnecting {}: {}", remoteAddress, e.getMessage() );
        }
    }
    
    private class ReaderThread extends Thread {
        private final BufferedReader fromNode;
        
        private volatile boolean running;
        
        private ReaderThread( BufferedReader readerFromNode ) {
            this.fromNode = readerFromNode;
        }

        private void dontRunAnymore() {
            running = false;
        }

        @Override
        public void run() {
            running = true;
            try {
                while ( running ) {
                    String input = fromNode.readLine();
                    log.debug( "Incoming from {}: {}", remoteAddress, input );
                    if ( input == null ) {
                        log.debug( "EOF from {}", remoteAddress );
                        disconnect();
                        return;
                    }
                    JSONObject message = new JSONObject( input );

                    String command = message.getString( "command" );
                    if ( command.equals( "ping" ) ) {
                        sendMessage( new JSONObject().put( "command", "pong" )
                                                     .put( "sequence-id", message.getInt( "sequence-id" )));
                    } else {
                        log.info( "Cannot understand request {} from {} - disconnecting", input, remoteAddress );
                        disconnect();
                    }
                }
            } catch ( IOException e ) {
                log.info( "Cannot receive message - connection to {} lost: {}", remoteAddress, e.getMessage() );
                disconnect();
            }
        }
    }
}
