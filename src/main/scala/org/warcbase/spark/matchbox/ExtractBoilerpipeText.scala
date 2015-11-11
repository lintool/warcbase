package org.warcbase.spark.matchbox

import java.io.IOException
import de.l3s.boilerpipe.extractors.DefaultExtractor

/**
 * UDF for extracting raw text content from an HTML page, minus "boilerplate"
 * content (using boilerpipe).
 */
object ExtractBoilerpipeText {
  def apply(input: String) = {
    if (input.isEmpty) Nil
    else
      try {
        val text = DefaultExtractor.INSTANCE.getText(input).replaceAll("[\\r\\n]+", " ").trim()
        if (text.isEmpty) Nil
        else text
      } catch {
        case e: Exception =>
          throw new IOException("Caught exception processing input row " + e)
      }
  }
}


