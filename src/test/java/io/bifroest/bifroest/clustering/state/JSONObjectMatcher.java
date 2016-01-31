package com.goodgame.profiling.graphite_bifroest.clustering.state;

import org.json.JSONArray;
import org.json.JSONObject;
import org.mockito.ArgumentMatcher;


public class JSONObjectMatcher extends ArgumentMatcher<JSONObject> {
    private final JSONObject pattern;

    public JSONObjectMatcher( JSONObject pattern ) {
        this.pattern = pattern;
    }

    @Override
    public boolean matches( Object argument ) {
        

        return matcherizeObjects( pattern, argument );
    }

    private boolean matcherizeObjects( JSONObject pattern, Object other ) {
        if ( !( other instanceof JSONObject ) ) return false;

        JSONObject co = (JSONObject) other;

        for ( String key : pattern.keySet() ) {
            if ( !co.has( key ) ) return false;
            Object patternValue = pattern.get( key );
            Object coValue = co.get( key );

            if ( patternValue instanceof ArgumentMatcher ) {
                ArgumentMatcher cpv = ( ArgumentMatcher ) patternValue;
                if ( !cpv.matches( coValue ) ) return false;
            } else if ( patternValue instanceof JSONObject ) {
                if ( !matcherizeObjects( (JSONObject) patternValue, coValue ) ) return false;
            } else if ( patternValue instanceof JSONArray ) {
                if ( !matcherizeArrays( (JSONArray) patternValue, coValue ) );
            } else {
                if ( !patternValue.equals( coValue ) ) return false;
            }
        }

        for ( String key : co.keySet() ) {
            if ( !pattern.has( key ) ) return false;
        }
        return true;
    }

    private boolean matcherizeArrays( JSONArray pattern, Object other ) {
        if ( !( other instanceof JSONArray ) ) return false;

        JSONArray co = (JSONArray) other;
        if ( pattern.length() != co.length() ) return false;

        for( int i = 0; i < pattern.length(); i++ ) {
            Object patternValue = pattern.get( i );
            Object coValue = co.get( i );

            if ( patternValue instanceof ArgumentMatcher ) {
                ArgumentMatcher cpv = ( ArgumentMatcher ) patternValue;
                if ( !cpv.matches( coValue ) ) return false;
            } else if ( patternValue instanceof JSONObject ) {
                if ( !matcherizeObjects( (JSONObject) patternValue, coValue ) ) return false;
            } else if ( patternValue instanceof JSONArray ) {
                if ( !matcherizeArrays( (JSONArray) patternValue, coValue ) );
            } else {
                if ( !patternValue.equals( coValue ) ) return false;
            }
        }
        return true;
    }
}
