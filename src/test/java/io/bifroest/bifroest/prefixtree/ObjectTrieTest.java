package io.bifroest.bifroest.prefixtree;

import io.bifroest.bifroest.prefixtree.ObjectTrie;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.Queue;

import org.junit.Before;
import org.junit.Test;

public class ObjectTrieTest {

    ObjectTrie<String, ?> trie;

    @Before
    public void initTrie() {
        trie = new ObjectTrie<>();
    }

    public void addEntry( String... strings ) {
        Queue<String> key = new LinkedList<String>();
        for ( String string : strings ) {
            key.add( string );
        }
        trie.add( key, null );
    }

    @Test
    public void testInitialState() {
        assertTrue( trie.isLeaf() );
        assertTrue( trie.children().isEmpty() );
    }

    @Test
    public void testEmptyEdd() {
        addEntry();

        assertTrue( trie.isLeaf() );
    }

    @Test
    public void testAddOneElement() {
        addEntry( "foo" );

        assertFalse( trie.isLeaf() );
        assertTrue( trie.get( "foo" ).isLeaf() );
    }

    @Test
    public void testAddSubElement() {
        addEntry( "foo", "bar" );

        assertFalse( trie.isLeaf() );
        assertFalse( trie.get( "foo" ).isLeaf() );
        assertTrue( trie.get( "foo" ).get( "bar" ).isLeaf() );
    }

    @Test
    public void testAddSubElementToExistingElement() {
        addEntry( "foo", "bar" );
        addEntry( "foo", "baz" );

        assertFalse( trie.isLeaf() );
        assertFalse( trie.get( "foo" ).isLeaf() );
        assertTrue( trie.get( "foo" ).get( "bar" ).isLeaf() );
        assertTrue( trie.get( "foo" ).get( "baz" ).isLeaf() );
    }

}
