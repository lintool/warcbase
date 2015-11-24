package org.warcbase.spark.matchbox

import java.io.ByteArrayInputStream

//import org.apache.pig.data.DataByteArray
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.ParseContext
import org.apache.tika.parser.pdf.PDFParser
import org.apache.tika.sax.BodyContentHandler;

object ExtractTextFromPDFs {
  val pdfParser = new PDFParser()

/*
  def apply(dba: DataByteArray): String = {
    if (dba.get.isEmpty) "N/A"
    else {
      try {
        val is = new ByteArrayInputStream(dba.get)
        val contenthandler = new BodyContentHandler(Integer.MAX_VALUE)
        val metadata = new Metadata()
        pdfParser.parse(is, contenthandler, metadata, new ParseContext())
        is.close()
        contenthandler.toString
      }
      catch {
        case t: Throwable =>
          t.printStackTrace()
          ""
      }
    }
  }
*/
}