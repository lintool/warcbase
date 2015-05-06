package org.warcbase.pig.piggybank;

import java.io.IOException;
import java.net.URL;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * UDF for extracting the top-level domain from an URL. Extracts the hostname from the first
 * argument; if it's <code>null</code>, extracts the hostname from the second argument. The second
 * argument is typically a source page, e.g., if the first URL is a relative URL, take the host from
 * the source page.
 */
public class ExtractTopLevelDomain extends EvalFunc<String> {
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0 || input.get(0) == null) {
      return null;
    }

    String host = null;
    try {
      host = (new URL((String) input.get(0))).getHost();
    } catch (Exception e) {
      // It's okay, just fall through here.
    }

    if (host != null || (host == null && input.size() == 0)) {
      return host;
    }

    try {
      return (new URL((String) input.get(1))).getHost();
    } catch (Exception e) {
      return null;
    }
  }
}