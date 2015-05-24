package org.warcbase.pig.piggybank;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;

public class ExtractTextFromPDFs extends EvalFunc<String> {
  private Parser pdfParser = new PDFParser();

  @Override
  public String exec(Tuple input) throws IOException {
    try {
      if (input == null || input.size() == 0 || input.get(0) == null) {
        return "N/A";
      }

      DataByteArray dba = (DataByteArray) input.get(0);
      InputStream is = new ByteArrayInputStream(dba.get());

      ContentHandler contenthandler = new BodyContentHandler(Integer.MAX_VALUE);
      Metadata metadata = new Metadata();

      pdfParser.parse(is, contenthandler, metadata, new ParseContext());

      if (is != null) {
        is.close();
      }

      return contenthandler.toString();
    } catch (Throwable t) {
      // Basically, catch everything...
      t.printStackTrace();
      return null;
    }
  }
}