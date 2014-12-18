package org.warcbase.pig.piggybank;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class ExtractTextFromPDFs extends EvalFunc<String> {
  private Parser pdfparser = new PDFParser();

  @Override
  public String exec(Tuple input) throws IOException {
    StringBuilder pdfText = new StringBuilder();

    if (input == null || input.size() == 0 || input.get(0) == null) {
      return "N/A";
    }

    // prepare input for use with Tika
    DataByteArray dba = (DataByteArray) input.get(0);
    InputStream is = new ByteArrayInputStream(dba.get());

    // set up Tika constructs
    ContentHandler contenthandler = new BodyContentHandler(Integer.MAX_VALUE);
    Metadata metadata = new Metadata();

    try {
      // parse the bytestream of the PDF into plain text
      pdfparser.parse(is, contenthandler, metadata, new ParseContext());
    } catch (SAXException | TikaException e) {
      e.printStackTrace();
    }

    // append the PDF's plain text to the string
    pdfText.append(contenthandler.toString());

    // close the input stream
    if (is != null) {
      is.close();
    }

    // return the plain text of the PDF
    return pdfText.toString();
  }
}