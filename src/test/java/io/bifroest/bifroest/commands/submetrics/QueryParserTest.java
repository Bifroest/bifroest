package io.bifroest.bifroest.commands.submetrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.bifroest.bifroest.commands.submetrics.QueryParser;

public class QueryParserTest {

    @Test
    public void testSimple() {
        String query = "foo";

        List<String> expected = Arrays.asList("foo");

        List<String> result = (new QueryParser(query)).parse();

        assertEquals(expected, result);
    }

    @Test
    public void testOneFunction() {
        String query = "foo(S1,S2,S3)";

        List<String> expected = Arrays.asList("S1", "S2", "S3");

        List<String> result = (new QueryParser(query)).parse();

        assertEquals(expected, result);
    }

    @Test
    public void testOneFunctionWithSpaces() {
        String query = "foo ( S1 , S2 , S3 )";

        List<String> expected = Arrays.asList("S1", "S2", "S3");

        List<String> result = (new QueryParser(query)).parse();

        assertEquals(expected, result);
    }

    @Test
    public void testNestedFunctions() {
        String query = "foo(bar(S1,S2), baz(S3,S4))";

        List<String> expected = Arrays.asList("S1", "S2", "S3", "S4");

        List<String> result = (new QueryParser(query)).parse();

        assertEquals(expected, result);
    }

    @Test
    public void testStrings() {
        String query = "foo(a\",\"b)";

        List<String> expected = Arrays.asList("a\",\"b");

        List<String> result = (new QueryParser(query)).parse();

        assertEquals(expected, result);
    }

    @Test
    public void testCurlyBraces() {
        String query = "foo(a{b,c}d)";

        List<String> expected = Arrays.asList("a{b,c}d");

        List<String> result = (new QueryParser(query)).parse();

        assertEquals(expected, result);
    }

    @Test(timeout = 1000, expected=Exception.class)
    public void testRealWorld2() {
        String query = "{sum*";

        (new QueryParser(query)).parse();
    }

    @Test(timeout = 1000, expected=Exception.class)
    public void testRealWorld3() {
        String query = "\"sum*";

        (new QueryParser(query)).parse();
    }
}
