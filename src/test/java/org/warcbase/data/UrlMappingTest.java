/*
 * Warcbase: an open-source platform for managing web archives
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.warcbase.data;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.INPUT_TYPE;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.junit.Before;
import org.junit.Test;

// This class aims to test the PrefixSearch functionality.

public class UrlMappingTest {
  private UrlMapping map;

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
    map = new UrlMapping(fst);
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

    results = map.prefixSearch("dad");
    assertEquals(0, results.size());
  }

  @Test
  public void testGetIdRange() throws IOException{
    int[] range;

    range = map.getIdRange("doga", "dogs");
    assertEquals(4, range[0]);
    assertEquals(6, range[1]);
    assertEquals("doga", map.getUrl(range[0]));
    assertEquals("dogs", map.getUrl(range[1]));

    range = map.getIdRange("doga", "dogb");
    assertEquals(4, range[0]);
    assertEquals(5, range[1]);
    assertEquals("doga", map.getUrl(range[0]));
    assertEquals("dogb", map.getUrl(range[1]));

    range = map.getIdRange("dogs", "dogs");
    assertEquals(6, range[0]);
    assertEquals(6, range[1]);
    assertEquals("dogs", map.getUrl(range[0]));
    assertEquals("dogs", map.getUrl(range[1]));

    // If either one of the bounds is invalid, expect null
    range = map.getIdRange("dog", "dogx");
    assertEquals(null, range);

    range = map.getIdRange("doga", "dogx");
    assertEquals(null, range);

    range = map.getIdRange("dog", "dogs");
    assertEquals(null, range);

    range = map.getIdRange("", "dogs");
    assertEquals(null, range);

    range = map.getIdRange("", "");
    assertEquals(null, range);

    range = map.getIdRange(null, "");
    assertEquals(null, range);

    range = map.getIdRange(null, null);
    assertEquals(null, range);

    range = map.getIdRange(null, null);
    assertEquals(null, range);
  }
}
