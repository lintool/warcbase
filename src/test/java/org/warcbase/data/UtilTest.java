package org.warcbase.data;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class UtilTest {

  @Test
  public void testGetIds() {
    String url = "http://www.house.gov/mthompson/the_1st_district.htm";
    String rowKey = "gov.house.www/mthompson/the_1st_district.htm";

    assertEquals(rowKey, Util.reverseHostname(url));
    assertEquals(url, Util.reverseBacUri(rowKey));
  }
}
