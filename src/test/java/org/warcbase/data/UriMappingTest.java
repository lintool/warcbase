package org.warcbase.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.fst.FST.INPUT_TYPE;
import org.warcbase.data.UriMapping;
import org.junit.Before;
import org.junit.Test;

// This class aims to test the PrefixSearch functionality.

public class UriMappingTest {
  private UriMapping map;

  @Before
  public void setUp() throws Exception {
    String inputValues[] = { "cat", "catch", "cut", "doga", "dogb", "dogs" };
    long outputValues[] = { 1, 2, 3, 4, 5, 6 };
    
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    Builder<Long> builder = new Builder<Long>(INPUT_TYPE.BYTE1, outputs);
    BytesRef scratchBytes = new BytesRef();
    IntsRef scratchInts = new IntsRef();
    for (int i = 0; i < inputValues.length; i++) {
      scratchBytes.copyChars(inputValues[i]);
      builder.add(Util.toIntsRef(scratchBytes, scratchInts), outputValues[i]);
    }
    FST<Long> fst = builder.finish();
    map = new UriMapping(fst);
  }

  @Test
  public void testGetIds() {
    assertEquals(-1, map.getID("apple"));
    assertEquals(1, map.getID("cat"));
    assertEquals(2, map.getID("catch"));
    assertEquals(3, map.getID("cut"));
    assertEquals(-1, map.getID("cuttery"));
    assertEquals(4, map.getID("doga"));
    assertEquals(5, map.getID("dogb"));
    assertEquals(6, map.getID("dogs"));
    assertEquals(-1, map.getID("dogz"));
  }

  @Test
  public void testGetUrls() {
    assertEquals(null, map.getUrl(0));
    assertEquals("cat", map.getUrl(1));
    assertEquals("catch", map.getUrl(2));
    assertEquals("cut", map.getUrl(3));
    assertEquals("doga", map.getUrl(4));
    assertEquals("dogb", map.getUrl(5));
    assertEquals("dogs", map.getUrl(6));
    assertEquals(null, map.getUrl(7));
  }

  @Test
  public void testPrefixSearch() {
    List<String> results;

    results = map.prefixSearch("cut");
    assertEquals(1, results.size());
    assertEquals("cut", results.get(0));

    results = map.prefixSearch("dog");
    assertEquals(3, results.size());
    assertEquals("doga", results.get(0));
    assertEquals("dogb", results.get(1));
    assertEquals("dogs", results.get(2));

    results = map.prefixSearch("");
    assertEquals(0, results.size());

    results = map.prefixSearch(null);
    assertEquals(0, results.size());

    // Broken test case.
    results = map.prefixSearch("dad");
    assertEquals(1, results.size());
  }

  @Test
  public void testGetIdRange() throws IOException{
    List<String> results;
    Long[] range;

    results = map.prefixSearch("dog");
    range = map.getIdRange(results);
    assertTrue(range[0] == 4);
    assertTrue(range[1] == 6);
  }
}
