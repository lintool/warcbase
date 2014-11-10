package org.warcbase.pig.piggybank;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;


public class ExtractTextFromPDFs extends EvalFunc<String> {
  
  @Override
  public String exec(Tuple input) throws IOException {
      String pdfText;

      if (input == null || input.size() == 0 || input.get(0) == null) {
          return "N/A";
      }
      String content = (String) input.get(0);
      
      if (content.isEmpty()) return "EMPTY";
      
      InputStream is = new ByteArrayInputStream(content.getBytes());
      ContentHandler contenthandler = new BodyContentHandler();
      Metadata metadata = new Metadata();
      Parser pdfparser = new AutoDetectParser();
      try {
        pdfparser.parse(is, contenthandler, metadata, new ParseContext());
      } catch (SAXException | TikaException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      pdfText = contenthandler.toString();

      return pdfText;
  }
  
}