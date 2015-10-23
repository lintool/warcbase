package org.warcbase.spark.matchbox

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.warcbase.spark.rdd.WARecordRDD.ToExtract._
import org.warcbase.spark.rdd.WARecordRDD._

class Spec extends FunSuite {

  private val master = "local[2]"
  private val appName = "example-spark"
  private var sc: SparkContext = _

  test("filter") {
    try {
      val conf = new SparkConf()
        .setMaster(master)
        .setAppName(appName)
      sc = new SparkContext(conf)

      val r = RecordLoader.loadArc("collections/ARCHIVEIT-227-UOFTORONTO-CANPOLPINT-20090201174320-00056-crawling04.us.archive.org.arc.gz", sc)
        .keepMimeTypes(Set("text/html"))
        .discardDate(null)
        .keepDomains(Set("greenparty.ca"))
        .extract(CRAWLDATE, DOMAIN)
    } catch {
      case e: Throwable => throw e
    }
    finally {
      if (sc != null) {
        sc.stop()
      }
    }
  }
}