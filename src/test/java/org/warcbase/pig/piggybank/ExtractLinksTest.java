package org.warcbase.pig.piggybank;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class ExtractLinksTest {
  private TupleFactory tupleFactory = TupleFactory.getInstance();

  @Test
  public void test1() throws IOException {
    ExtractLinks udf = new ExtractLinks();

    String fragment = "Here is <a href=\"http://www.google.com\">a search engine</a>.\n" +
        "Here is <a href=\"http://www.twitter.com/\">Twitter</a>.\n";

    DataBag bag = udf.exec(tupleFactory.newTuple(fragment));
    assertEquals(2, bag.size());

    Tuple tuple = null;
    Iterator<Tuple> iter = bag.iterator();
    tuple = iter.next();
    assertEquals("http://www.google.com", (String) tuple.get(0));
    assertEquals("a search engine", (String) tuple.get(1));

    tuple = iter.next();
    assertEquals("http://www.twitter.com/", (String) tuple.get(0));
    assertEquals("Twitter", (String) tuple.get(1));
  }

  @Test
  public void test2() throws IOException {
    ExtractLinks udf = new ExtractLinks();

    String fragment = "Here is <a href=\"http://www.google.com\">a search engine</a>.\n" +
        "Here is <a href=\"page.html\">a relative URL</a>.\n";

    DataBag bag = udf.exec(tupleFactory.newTuple(Arrays.asList(fragment, "http://www.foobar.org/index.html")));
    assertEquals(2, bag.size());

    Tuple tuple = null;
    Iterator<Tuple> iter = bag.iterator();
    tuple = iter.next();
    assertEquals("http://www.google.com", (String) tuple.get(0));
    assertEquals("a search engine", (String) tuple.get(1));

    tuple = iter.next();
    assertEquals("http://www.foobar.org/page.html", (String) tuple.get(0));
    assertEquals("a relative URL", (String) tuple.get(1));
  }

}
