package com.goodgame.profiling.graphite_bifroest.clustering;

import java.util.Arrays;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.goodgame.profiling.bifroest.bifroest_client.metadata.PortMap;
import com.goodgame.profiling.commons.util.json.JSONUtils;
import com.goodgame.profiling.graphite_bifroest.commands.GetMetricSetCommand;
import com.goodgame.profiling.graphite_bifroest.commands.GetSubMetricsCommand;
import com.goodgame.profiling.graphite_bifroest.commands.GetValueCommand;
import com.goodgame.profiling.graphite_bifroest.commands.IncludeMetrics;

public final class BifroestPortmap implements PortMap {
    private int clusterPort;
    private int includeMetricPort;
    private int fastIncludeMetricPort;
    private int getMetricPort;
    private int getMetricSetPort;
    private int getSubmetricPort;

    @Override
    public int getClusterPort() {
        return clusterPort;
    }

    public void setClusterPort( int clusterPort ) {
        this.clusterPort = clusterPort;
    }

    @Override
    public int getIncludeMetricPort() {
        return includeMetricPort;
    }

    public void setIncludeMetricPort( int includeMetricPort ) {
        this.includeMetricPort = includeMetricPort;
    }

    @Override
    public int getFastIncludeMetricPort() {
        return fastIncludeMetricPort;
    }

    public void setFastIncludeMetricPort( int fastIncludeMetricPort ) {
        this.fastIncludeMetricPort = fastIncludeMetricPort;
    }

    @Override
    public int getMetricPort() {
        return getMetricPort;
    }

    public void setGetMetricPort( int getMetricPort ) {
        this.getMetricPort = getMetricPort;
    }

    @Override
    public int getMetricSetPort() {
        return getMetricSetPort;
    }

    public void setGetMetricSetPort( int getMetricSetPort ) {
        this.getMetricSetPort = getMetricSetPort;
    }

    @Override
    public int getSubmetricPort() {
        return getSubmetricPort;
    }

    public void setGetSubmetricPort( int getSubmetricPort ) {
        this.getSubmetricPort = getSubmetricPort;
    }

    @Override
    public boolean equals( Object other ) {
        return this.equals_( other );
    }

    @Override
    public int hashCode() {
        return this.hashCode_();
    }

    @Override
    public String toString() {
        return "BifroestPortmap{" + "clusterPort=" + clusterPort + ", includeMetricPort=" + includeMetricPort + ", fastIncludeMetricPort=" + fastIncludeMetricPort + ", getMetricPort=" + getMetricPort + ", getMetricSetPort=" + getMetricSetPort + ", getSubmetricPort=" + getSubmetricPort + '}';
    }

    public static PortMap fromConfig( JSONObject config ) {
        BifroestPortmap result = new BifroestPortmap();
        getClusterPortFrom( config, result );
        getMultiserverPortsFrom( config, result );
        getNettyPortFrom( config, result );
        return result;
    }

    private static void getClusterPortFrom( JSONObject config, BifroestPortmap map ) {
        if ( config.has( "clustering" ) ) {
            map.setClusterPort( config.getJSONObject( "clustering" ).getInt( "cluster-port" ) );
        }
    }

    private static void getMultiserverPortsFrom( JSONObject config, BifroestPortmap map ) {
        if ( config.has( "multi-server" ) ) {
            JSONObject multiserverConfig = config.getJSONObject( "multi-server" );
            JSONArray interfaces = multiserverConfig.getJSONArray( "interfaces" );
            JSONObject allInterface = findAllInterface( interfaces );
            map.setGetMetricPort( findPort( GetValueCommand.COMMAND, interfaces, allInterface ) );
            map.setGetMetricSetPort( findPort( GetMetricSetCommand.COMMAND, interfaces, allInterface ) );
            map.setGetSubmetricPort( findPort( GetSubMetricsCommand.COMMAND, interfaces, allInterface ) );
            map.setIncludeMetricPort( findPort( IncludeMetrics.COMMAND, interfaces, allInterface ) );
        } else {
            map.setGetMetricPort( -1 );
            map.setGetMetricSetPort( -1 );
            map.setGetSubmetricPort( -1 );
            map.setIncludeMetricPort( -1 );
        }
    }

    private static void getNettyPortFrom( JSONObject config, BifroestPortmap map ) {
        if ( config.has( "netty" ) ) {
            map.setFastIncludeMetricPort( config.getJSONObject( "netty" ).getInt( "port" ) );
        } else {
            map.setFastIncludeMetricPort( -1 );
        }
    }

    private static JSONObject findAllInterface( JSONArray interfaces ) {
        for ( int i = 0; i < interfaces.length(); i++ ) {
            JSONObject interfaceConfig = interfaces.getJSONObject( i );
            try {
                String allString = interfaceConfig.getString( "commands" );
                if ( allString.equals( "all" ) ) {
                    return interfaceConfig;
                }
            } catch ( JSONException e  ) {
                // not a string, so not the string "all"
            }
        }
        throw new IllegalStateException( "Cannot find fallback all interface" );
    }

    private static int findPort( String command, JSONArray interfaces, JSONObject allInterface ) {
        for ( int i = 0; i < interfaces.length(); i++ ) {
            JSONObject interfaceConfig = interfaces.getJSONObject( i );
            try {
                String[] commands = JSONUtils.getStringArray( "commands", interfaceConfig );
                if ( Arrays.asList( commands ).contains( command ) ) {
                    return interfaceConfig.getInt( "port" );
                }
            } catch ( JSONException e  ) {
                // not a list, so not a list containing the command
            }
        }
        return allInterface.getInt( "port" );
    }
}
