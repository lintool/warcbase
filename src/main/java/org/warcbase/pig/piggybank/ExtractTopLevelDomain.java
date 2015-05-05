package org.warcbase.pig.piggybank;

import java.io.IOException;
import java.net.URL;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * UDF for extracting the top-level domain from an URL.
 */
public class ExtractTopLevelDomain extends EvalFunc<String> {
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0 || input.get(0) == null) {
      return null;
    }

    try {
      return (new URL((String) input.get(0))).getHost();
    } catch (Exception e) {
      return null;
    }
  }
}