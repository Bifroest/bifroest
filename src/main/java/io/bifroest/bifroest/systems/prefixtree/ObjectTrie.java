package com.goodgame.profiling.graphite_bifroest.systems.prefixtree;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

public final class ObjectTrie< K, V > {

    private final Map<K, ObjectTrie<K, V>> children;
    private V value;

    public ObjectTrie() {
        this.children = new ConcurrentHashMap<>();
        this.value = null;
    }

    public V value() {
        return value;
    }

    public boolean isLeaf() {
        return children.isEmpty();
    }

    public Set<Entry<K, ObjectTrie<K, V>>> children() {
        return children.entrySet();
    }

    public ObjectTrie<K, V> get( K key ) {
        return children.get( key );
    }

    public ObjectTrie<K, V> get( Queue<K> key ) {
        if ( key.isEmpty() ) {
            return this;
        } else if ( children.containsKey( key.peek() ) ) {
            return children.get( key.poll() ).get( key );
        } else {
            return null;
        }
    }

    public void add( Queue<K> key, V value ) {
        if ( key.isEmpty() ) {
            this.value = value;

        } else if ( children.containsKey( key.peek() ) ) {
            children.get( key.poll() ).add( key, value );

        } else {
            ObjectTrie<K, V> child = new ObjectTrie<>();
            ObjectTrie<K, V> alreadyPresentChild = children.putIfAbsent( key.poll(), child );
            if ( alreadyPresentChild == null ) {
                child.add( key, value );
            } else {
                alreadyPresentChild.add( key, value );
            }
        }
    }
    
    public boolean filterTree( Predicate<V> removeIfTrue ) {
        if ( this.isLeaf() ) {
            return removeIfTrue.test(value);
        } else {
            for ( Iterator<ObjectTrie<K,V>> valueIter = children.values().iterator(); valueIter.hasNext();  ) {
                ObjectTrie<K,V> value = valueIter.next();
                if ( value.filterTree( removeIfTrue ) ) {
                    valueIter.remove();
                }
            }
            return children.isEmpty();
        }
    }
    public void remove( ObjectTrie<K, V> trie ){
        for ( Iterator<ObjectTrie<K,V>> valueIter = children.values().iterator(); valueIter.hasNext();  ) {
            ObjectTrie<K,V> value = valueIter.next();
            if ( value == trie ) { // conscious identity comparison
                valueIter.remove();
                return;
            }
        }
    }

    @Deprecated
    public Collection<Queue<K>> collectKeys() {
        Collection<Queue<K>> accumulator = new ArrayList<>();
        Deque<K> stack = new ArrayDeque<>();
        collectKeys( stack, accumulator );
        return accumulator;
    }

    @Deprecated
    private void collectKeys( Deque<K> stack, Collection<Queue<K>> accumulator ) {
        if ( children.isEmpty() ) {
            // Reverse order
            LinkedList<K> result = new LinkedList<>();
            for ( K part : stack ) {
                result.addFirst( part );
            }
            accumulator.add( result );
        }
        for ( Entry<K, ObjectTrie<K, V>> child : children.entrySet() ) {
            stack.push( child.getKey() );
            child.getValue().collectKeys( stack, accumulator );
            stack.pop();
        }
    }

    public void forAllLeaves( BiConsumer<Collection<K>, V> consumer ) {
        Deque<K> deque = new ArrayDeque<>();
        forAllLeaves( consumer, deque );
    }

    private void forAllLeaves( BiConsumer<Collection<K>, V> consumer, Deque<K> deque ) {
        if ( children.isEmpty() ) {
            consumer.accept( deque, value );
        }
        for ( K childName : children.keySet() ) {
            deque.addLast( childName );
            children.get( childName ).forAllLeaves( consumer, deque );
            deque.removeLast();
        }
    }

    
}
