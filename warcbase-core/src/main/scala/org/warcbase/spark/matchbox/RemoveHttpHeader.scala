package org.warcbase.spark.matchbox

/**
  * Created by youngbinkim on 7/9/16.
  */
object RemoveHttpHeader {
  val headerEnd = "\r\n\r\n"
  def apply(content: String): String = {
    try {
      if (content.startsWith("HTTP/"))
        content.substring(content.indexOf(headerEnd) + headerEnd.length)
      else
        content
    } catch {
      case e: Exception => {
        println(e)
        null
      }
    }
  }
}