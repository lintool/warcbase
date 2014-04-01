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
    String inputValues[] = {"cat", "catch","cut", "doga","dogb","dogs"};
    long outputValues[] = {1,2,3,4,5,6};
    
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
  public void testPrefixSearch() throws Exception {
    List<String> results = map.prefixSearch("dog");
    assertTrue(results.size()==3);
    assertEquals("doga",results.get(0));
    assertEquals("dogb",results.get(1));
    assertEquals("dogs",results.get(2));
  }
  
  @Test
  public void testGetIdRange() throws IOException{
    List<String> results = map.prefixSearch("dog");
    Long[] range = map.getIdRange(results);
    assertTrue(range[0] == 4);
    assertTrue(range[1] == 6);
  }
}
