package org.warcbase.pig.piggybank;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.jsoup.Jsoup;

/**
 * UDF for extracting raw text content from an HTML page (using Jsoup).
 */
public class ExtractRawText extends EvalFunc<String> {
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0 || input.get(0) == null) {
      return null;
    }

    try {
      // Use Jsoup for cleanup.
      return Jsoup.parse((String) input.get(0)).text().replaceAll("[\\r\\n]+", " ");
    } catch (Exception e) {
      throw new IOException("Caught exception processing input row ", e);
    }
  }
}