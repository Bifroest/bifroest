package com.goodgame.profiling.graphite_bifroest.systems.prefixtree;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;

public class FindAgeTest {
    private PrefixTree tree;

    @Before
    public void setUp() {
        tree = new PrefixTree( 42 );
        tree.addEntry( "a.b.d.e", 1 );
        tree.addEntry( "a.c", 2 );
        tree.addEntry( "a.b.foo.grr", 3 );
        tree.addEntry( "a.b.foo.house", 4 );
    }

    @Test
    public void testLeaves() {
        assertEquals( 1, tree.findAge( "a.b.d.e", Collections.<String> emptyList() ) );
        assertEquals( 2, tree.findAge( "a.c", Collections.<String> emptyList() ) );
        assertEquals( 3, tree.findAge( "a.b.foo.grr", Collections.<String> emptyList() ) );
        assertEquals( 4, tree.findAge( "a.b.foo.house", Collections.<String> emptyList() ) );
    }

    @Test
    public void testNodes() {
        assertEquals( 1, tree.findAge( "a", Collections.<String> emptyList() ) );
        assertEquals( 1, tree.findAge( "a.b", Collections.<String> emptyList() ) );
        assertEquals( 3, tree.findAge( "a.b.foo", Collections.<String> emptyList() ) );
    }

    @Test
    public void testMissing() {
        assertEquals( Long.MAX_VALUE, tree.findAge( "b", Collections.<String> emptyList() ) );
        assertEquals( Long.MAX_VALUE, tree.findAge( "a.b.c", Collections.<String> emptyList() ) );
    }

    @Test
    public void testBlacklisted() {
        assertEquals( 3, tree.findAge( "a.b", Arrays.asList( "d.e" ) ) );
        assertEquals( 4, tree.findAge( "a.b.foo", Arrays.asList( "grr" ) ) );
        assertEquals( Long.MAX_VALUE, tree.findAge( "a.b", Arrays.asList( "d.e", "foo.grr", "foo.house" ) ) );
        assertEquals( Long.MAX_VALUE, tree.findAge( "a.b", Arrays.asList( "d", "foo" ) ) );
    }

}
