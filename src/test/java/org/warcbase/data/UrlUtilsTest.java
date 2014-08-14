package org.warcbase.data;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class UrlUtilsTest {
  @Test
  public void test1() {
    String url = "http://www.house.gov/mthompson/the_1st_district.htm";
    String rowKey = "gov.house.www/mthompson/the_1st_district.htm";

    assertEquals(rowKey, UrlUtils.urlToKey(url));
    assertEquals(url, UrlUtils.keyToUrl(rowKey));
  }

  @Test
  public void test2() {
    String[] hostnames = new String[] { "www.house.gov", "umiacs.umd.edu", "foo.bar.com:8080",
        "a.b.c.d.com:12345", "warcbase.org", "foo" };
    String[] reversed = new String[] { "gov.house.www", "edu.umd.umiacs", "com.bar.foo:8080", 
        "com.d.c.b.a:12345", "org.warcbase", "foo" };

    for (int i=0; i<hostnames.length; i++) {
      assertEquals(reversed[i], UrlUtils.reverseHostname(hostnames[i]));
      assertEquals(hostnames[i], UrlUtils.reverseHostname(UrlUtils.reverseHostname(hostnames[i])));
    }
  }
}
