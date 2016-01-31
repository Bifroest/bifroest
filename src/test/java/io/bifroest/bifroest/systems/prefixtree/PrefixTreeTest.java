package io.bifroest.bifroest.systems.prefixtree;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class PrefixTreeTest {
    @Test
    public void test() {
        PrefixTree tree = new PrefixTree( 42 );
        tree.addEntry("foo.bar1.baz");
        tree.addEntry("foo.bar2.baz");
        tree.addEntry("foo.bar3.baz");
        
        Collection<Pair<String, Boolean>> result = new HashSet<>(tree.getPrefixesMatching("foo.{bar1,bar2,bar3}.baz"));
        
        Collection<Pair<String, Boolean>> expected = new HashSet<>(Arrays.<Pair<String, Boolean>>asList(
                new ImmutablePair<String, Boolean>("foo.bar1.baz", true),
                new ImmutablePair<String, Boolean>("foo.bar2.baz", true),
                new ImmutablePair<String, Boolean>("foo.bar3.baz", true)
                ));
        
        assertEquals(expected, result);
    }

}
