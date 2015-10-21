package org.warcbase.spark.scripts

import org.apache.spark.{SparkConf, SparkContext}
import org.warcbase.spark.matchbox.ArcRecords
import org.warcbase.spark.matchbox.ArcRecords._

object Filter {
  def filter(sc: SparkContext) = {
    val r = ArcRecords.load("collections/ARCHIVEIT-227-UOFTORONTO-CANPOLPINT-20090201174320-00056-crawling04.us.archive.org.arc.gz", sc).keepMimeTypes(Set("text/html")).discardDate(null).keepDomains(Set("greenparty.ca")).extractUrlAndBody()
    r.saveAsTextFile("/green")
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Filter Test")
    val sc = new SparkContext(conf)
  }
}
