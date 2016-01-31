package io.bifroest.bifroest.systems.prefixtree;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

public class PrefixComputationTest {
    private PrefixTree tree;

    @Before
    public void setUp() {
        tree = new PrefixTree( 42 );
        tree.addEntry( "a.b.d.e" );
        tree.addEntry( "a.c" );
        tree.addEntry( "a.b.foo.grr" );
        tree.addEntry( "a.b.foo.house" );
    }

    @Test
    public void test1() {
        List<Pair<String, Boolean>> result = tree.getPrefixesMatching( "*" );
        List<Pair<String, Boolean>> expected = new ArrayList<>();
        expected.add( new ImmutablePair<>( "a", false ) );

        assertEquals( expected, result );
    }

    @Test
    public void test2() {
        List<Pair<String, Boolean>> result = tree.getPrefixesMatching( "a.*" );
        List<Pair<String, Boolean>> expected = new ArrayList<>();
        expected.add( new ImmutablePair<>( "a.b", false ) );
        expected.add( new ImmutablePair<>( "a.c", true ) );

        assertEquals( expected, result );
    }

    @Test
    public void test5() {
        List<Pair<String, Boolean>> result = tree.getPrefixesMatching( "a.b.*.*" );
        List<Pair<String, Boolean>> expected = new ArrayList<>();
        expected.add( new ImmutablePair<>( "a.b.d.e", true ) );
        expected.add( new ImmutablePair<>( "a.b.foo.grr", true ) );
        expected.add( new ImmutablePair<>( "a.b.foo.house", true ) );

        assertEquals( expected, result );
    }

    @Test
    public void test6() {
        List<Pair<String, Boolean>> result = tree.getPrefixesMatching( "*.*.*.*" );
        List<Pair<String, Boolean>> expected = new ArrayList<>();
        expected.add( new ImmutablePair<>( "a.b.d.e", true ) );
        expected.add( new ImmutablePair<>( "a.b.foo.grr", true ) );
        expected.add( new ImmutablePair<>( "a.b.foo.house", true ) );

        assertEquals( expected, result );
    }

    @Test
    public void testCurlyBraces() {
        List<Pair<String, Boolean>> result = tree.getPrefixesMatching( "a.{b,c}" );
        List<Pair<String, Boolean>> expected = new ArrayList<>();
        expected.add( new ImmutablePair<>( "a.b", false ) );
        expected.add( new ImmutablePair<>( "a.c", true ) );

        assertEquals( expected, result );
    }

}
