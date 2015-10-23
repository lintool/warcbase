package org.warcbase.spark.matchbox

import java.io.ByteArrayInputStream

import org.apache.tika.Tika
import org.apache.tika.detect.DefaultDetector
import org.apache.tika.parser.AutoDetectParser
;

object DetectMimeTypeTika {
  def apply(magicFile: String, content: String): String = {
    if (magicFile.isEmpty) return "N/A"
    if (content.isEmpty) "EMPTY"
    else {
      val is = new ByteArrayInputStream(content.getBytes)
      val detector = new DefaultDetector()
      val parser = new AutoDetectParser(detector)
      val mimetype = new Tika(detector, parser).detect(is)
      mimetype
    }
  }
}
