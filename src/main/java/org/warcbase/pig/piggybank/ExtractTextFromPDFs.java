package org.warcbase.pig.piggybank;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
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
      
      DataByteArray dba = (DataByteArray)input.get(0);
      
     // String content = (String) input.get(0); //failed due to being unable to cast DataByteArray to String
      
      //if (content.isEmpty()) return "EMPTY";
      
      //InputStream is = new ByteArrayInputStream(content.getBytes());
      InputStream is = new ByteArrayInputStream(dba.get());
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