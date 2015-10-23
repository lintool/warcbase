package org.warcbase.spark.matchbox

import java.net.URL

object ExtractTopLevelDomain {
  def apply(url: String, source: String = ""): String = {
    if (url == null) return null
    try {
      val u = new URL(url)
      if (u.getHost == null) {
        val s = new URL(source)
        s.getHost
      } else {
        u.getHost
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        ""
    }
  }
}
