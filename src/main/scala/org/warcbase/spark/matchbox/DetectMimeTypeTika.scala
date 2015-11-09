package org.warcbase.spark.matchbox

import java.io.ByteArrayInputStream

import org.apache.tika.Tika
import org.apache.tika.detect.DefaultDetector
import org.apache.tika.parser.AutoDetectParser

/**
  * A UDF to detect mime types
  */
object DetectMimeTypeTika {
  def apply(content: String): String = {
    if (content.isEmpty) "N/A"
    else {
      val is = new ByteArrayInputStream(content.getBytes)
      val detector = new DefaultDetector()
      val parser = new AutoDetectParser(detector)
      val mimetype = new Tika(detector, parser).detect(is)
      mimetype
    }
  }
}
