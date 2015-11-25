package org.warcbase.spark.scripts

import org.apache.spark.SparkContext
import org.warcbase.spark.matchbox.{RemoveHTML, RecordLoader}
import org.warcbase.spark.rdd.RecordRDD._

object Filter {
  def filter(sc: SparkContext) = {
    val r = RecordLoader.loadArc("collections/ARCHIVEIT-227-UOFTORONTO-CANPOLPINT-20090201174320-00056-crawling04.us.archive.org.arc.gz", sc)
      .keepMimeTypes(Set("text/html"))
      .discardDate(null)
      .keepDomains(Set("greenparty.ca"))
      .map(r => (r.getCrawldate, RemoveHTML(r.getContentString)))
    r.saveAsTextFile("/green")
  }
}

