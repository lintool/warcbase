package org.warcbase.spark.matchbox

import java.io.IOException

import org.jsoup.Jsoup

object RemoveHTML {
  def apply(content: String) = {
    try {
      Jsoup.parse(content).text().replaceAll("[\\r\\n]+", " ")
    }
    catch {
      case e: Exception => throw new IOException("Caught exception processing input row ", e)
    }
  }
}
