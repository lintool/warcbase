package org.warcbase.spark.matchbox

import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup
import org.warcbase.spark.archive.io.ArchiveRecord
import org.warcbase.spark.rdd.RecordRDD._


/**
  * Extract Plain text ..
  * output will contain many lines of "url cleanedPlainText" for each ArchiveRecord
  */
object ExtractPlainText {
  def apply(records: RDD[ArchiveRecord], output: String) = {
    records.keepValidPages()
      .map(x=>(x.getUrl, (Jsoup.parse(x.getContentString).body().text().replaceAll("[\\r\\n]", "\\t"), x.getCrawlDate.toFloat)))
      .reduceByKey((pair1, pair2) => {
        // remote duplicate urls... take latest version
        if (pair1._2 > pair2._2) pair1
        else pair2
      })
      .map(x => x._1 + "\t" + x._2._1)
      .saveAsTextFile(output)
  }
}

