package org.warcbase.pig.piggybank;

import java.io.IOException;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.common.collect.Lists;

/**
 * UDF for extracting links from a webpage given the HTML content (using Jsoup). Returns a bag of
 * tuples, where each tuple consists of the URL and the anchor text.
 */
public class ExtractLinks extends EvalFunc<DataBag> {
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private static final BagFactory BAG_FACTORY = BagFactory.getInstance();
  
  public DataBag exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0 || input.get(0) == null) {
      return null;
    }

    try {
      String html = (String) input.get(0);
      String base = input.size() > 1 ? (String) input.get(1) : null;

      DataBag output = BAG_FACTORY.newDefaultBag();
      Document doc = Jsoup.parse(html);
      Elements links = doc.select("a[href]");

      for (Element link : links) {
        if (base != null) {
          link.setBaseUri(base);
        }
        String target = link.attr("abs:href");
        if (target.length() == 0) {
          continue;
        }

        // Create each tuple (URL, anchor text)
        List<String> linkTuple = Lists.newArrayList();
        linkTuple.add(target);
        linkTuple.add(link.text());
        output.add(TUPLE_FACTORY.newTupleNoCopy(linkTuple));
      }
      return output;
    } catch (Exception e) {
      throw new IOException("Caught exception processing input row ", e);
    }
  }
}