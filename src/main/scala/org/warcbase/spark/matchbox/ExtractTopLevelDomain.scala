package org.warcbase.spark.matchbox

import java.net.URL

object ExtractTopLevelDomain {
  def apply(url: String, source: String = ""): String = {
    if (url == null) return null
    var host: String = null
    try {
      host = new URL(url).getHost
    } catch {
      case e: Exception => // it's okay
    }
    if (host != null || source == null) return host
    try {
      new URL(source).getHost
    } catch {
      case e: Exception => null
    }
  }
}
