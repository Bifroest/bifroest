package io.bifroest.bifroest.systems.prefixtree;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import io.bifroest.commons.statistics.ProgramStateChanged;

public final class PrefixTree {
    private static final Logger log = LogManager.getLogger();
    
    private static final Pattern replaceBracesPattern = Pattern.compile( "\\{([^}]+?)\\}" );

    private final ObjectTrie<String, Long> trie = new ObjectTrie<>();
    
    private long nameRetention;
    
    public PrefixTree( long nameRetention ) {
        this.nameRetention = nameRetention;
    }

    private static Queue<String> stringToKey( String string ) {
        Queue<String> queue = new LinkedList<>();
        if ( !string.isEmpty() ) {
            String[] array = string.split( "\\." );
            for ( int i = 0; i < array.length; i++ ) {
                queue.add( array[i] );
            }
        }
        return queue;
    }

    public void addEntry( String metricName ) {
        trie.add( stringToKey( metricName ), System.currentTimeMillis() / 1000 );
    }

    public void addEntry( String metricName, long timestamp ) {
        trie.add( stringToKey( metricName ), timestamp );
    }

    public long findAge( final String prefix, final List<String> blacklist ) {
        log.entry( prefix, blacklist );
        ProgramStateChanged.fireContextChangeToState("bifroest-bifroest-getAgeCommand", "RegexBlacklistStream");
        String regex = blacklist.stream()
                                .map( s -> StringUtils.replace( s, ".", "\\." ) )
                                .collect( Collectors.joining( ")|(", "(^|\\.)(", ")($|\\.)" ) );
        log.debug( "Regex: {}", regex );
        final Pattern p = Pattern.compile( regex );

        // Misuse AtomicLong as a LongHolder
        AtomicLong l = new AtomicLong( Long.MAX_VALUE );
        ProgramStateChanged.fireContextChangeToState("bifroest-bifroest-getAgeCommand", "EnterForAllLeaves");
        forAllLeaves( (metric, age) -> {
            if ( !blacklist.isEmpty() && p.matcher(metric).find() ) {
                log.debug( "{} is blacklisted.", metric );
            } else {
                if ( age < l.get() ) {
                    l.set( age );
                    log.trace( "Setting age to {} because of metric {}", age, metric );
                }
            }
        }, prefix );

        return log.exit( l.get() );
    }

    @Deprecated
    public Iterable<String> collectLeaves() {
        return new Iterable<String>() {

            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {

                    Iterator<Queue<String>> iter = trie.collectKeys().iterator();

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public String next() {
                        return StringUtils.join( iter.next(), '.' );
                    }

                    @Override
                    public void remove() {
                        iter.remove();
                    }
                };
            }
        };
    }

    public List<Pair<String, Boolean>> getPrefixesMatching( String query ) {

        String[] queryParts = StringUtils.splitPreserveAllTokens( query, '.' );
        Pattern[] queryPartPatterns = new Pattern[queryParts.length];
        List<Pair<String, Boolean>> results = new ArrayList<>();

        for ( int i = 0; i < queryParts.length; i++ ) {
            String queryPart = queryParts[i];

            queryPart = StringUtils.replace( queryPart, "*", ".*" );
            queryPart = replaceBraces( queryPart );
            queryPart = "^" + queryPart + "$";
            queryPartPatterns[i] = Pattern.compile( queryPart );
        }
        ProgramStateChanged.fireContextChangeToState("bifroest-bifroest-getSubMetrics", "TreeCollectNodes");
        collectNodes( trie, 0, queryPartPatterns, new String[queryParts.length], results );

        return results;
    }

    private void collectNodes( ObjectTrie<String, Long> node, int depth, Pattern[] queryPartPatterns, String[] matchedPatterns,
            List<Pair<String, Boolean>> results ) {
        if ( depth == queryPartPatterns.length ) {
            results.add( new ImmutablePair<String, Boolean>( StringUtils.join( matchedPatterns, ".", 0, depth ), node.isLeaf() ) );
            return;
        }

        for ( Entry<String, ObjectTrie<String, Long>> entry : node.children() ) {
            if ( queryPartPatterns[depth].matcher( entry.getKey() ).find() ) {
                matchedPatterns[depth] = entry.getKey();
                collectNodes( entry.getValue(), depth + 1, queryPartPatterns, matchedPatterns, results );
            }
        }
    }

    private static String replaceBraces( String input ) {
        StringBuffer result = new StringBuffer();

        Matcher m = replaceBracesPattern.matcher( input );

        while ( m.find() ) {
            StringBuilder replacement = new StringBuilder();
            replacement.append( '(' );
            replacement.append( m.group( 1 ).replace( ',', '|' ) );
            replacement.append( ')' );
            m.appendReplacement( result, replacement.toString() );
        }
        m.appendTail( result );

        return result.toString();
    }

    @Deprecated
    public static JSONObject toJSONObject( PrefixTree tree, String prefix ) {
        ObjectTrie<String, Long> subTrie = tree.trie.get( stringToKey( prefix ) );
        if ( subTrie == null ) {
            // Empty tree, if prefix not found
            return new JSONObject();
        } else {
            return toJSONObject( subTrie );
        }
    }

    @Deprecated
    public static JSONObject toJSONObject( PrefixTree tree ) {
        return toJSONObject( tree.trie );
    }

    @Deprecated
    private static JSONObject toJSONObject( ObjectTrie<String, Long> trie ) {
        JSONObject result = new JSONObject();
        if ( trie.isLeaf() && trie.value() != null ) {
            result.put( "age", trie.value() );
        } else {
            for ( Entry<String, ObjectTrie<String, Long>> entry : trie.children() ) {
                result.put( entry.getKey(), toJSONObject( entry.getValue() ) );
            }
        }
        return result;
    }

    private static void forAllLeaves( BiConsumer<String, Long> consumer, ObjectTrie<String, Long> trie ) {
        trie.forAllLeaves(
            ( keyparts, value ) ->
                consumer.accept( keyparts.stream().collect( Collectors.joining(".") ), value ) );
    }

    public void forAllLeaves( BiConsumer<String, Long> consumer ) {
        forAllLeaves( consumer, trie );
    }

    public void forAllLeaves( BiConsumer<String, Long> consumer, String prefix ) {
        ObjectTrie<String, Long> subTrie = trie.get( stringToKey( prefix ) );
        if ( subTrie != null ) {
            forAllLeaves( (partialmetricname, age) -> consumer.accept( prefix + "." + partialmetricname, age ), subTrie );
        }
    }

    public void removeOldObjects(){
        Instant now = Clock.systemUTC().instant();
        
        long nameRetention = now.minusSeconds( this.nameRetention ).getEpochSecond();
        log.trace("filtering tree for timestamps < {}", nameRetention);
        trie.filterTree((Long t) -> t < nameRetention);
    }

}
