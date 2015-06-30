package org.warcbase.pig.piggybank;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import de.l3s.boilerpipe.extractors.DefaultExtractor;
// Could also use tika.parser.html.BoilerpipeContentHandler, which uses older version of boilerpipe

/**
 * UDF for extracting raw text content from an HTML page, minus "boilerplate"
 * content (using boilerpipe).
 */
public class ExtractBoilerpipeText extends EvalFunc<String> {
  public String exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0 || input.get(0) == null) {
      return null;
    }

    try {
      // Other available extractors: https://boilerpipe.googlecode.com/svn/trunk/boilerpipe-core/javadoc/1.0/de/l3s/boilerpipe/extractors/package-summary.html
      String text = DefaultExtractor.INSTANCE.getText((String) input.get(0)).replaceAll("[\\r\\n]+", " ").trim();
      if (text.isEmpty())
        return null;
      else
        return text;
    } catch (Exception e) {
      throw new IOException("Caught exception processing input row ", e);
    }
  }
}
