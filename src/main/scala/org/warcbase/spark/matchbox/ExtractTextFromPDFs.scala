/*
 * Warcbase: an open-source platform for managing web archives
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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