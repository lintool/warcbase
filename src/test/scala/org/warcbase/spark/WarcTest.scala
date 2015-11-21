package org.warcbase.spark

import com.google.common.io.Resources
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.warcbase.spark.matchbox.RecordLoader
import org.warcbase.spark.matchbox.RecordTransformers.WARecord
import org.warcbase.spark.rdd.RecordRDD._

@RunWith(classOf[JUnitRunner])
class WarcTest extends FunSuite with BeforeAndAfter {

  private val warcPath = Resources.getResource("warc/example.warc.gz").getPath
  private val master = "local[2]"
  private val appName = "example-spark"
  private var sc: SparkContext = _
  private var records: RDD[WARecord] = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
    records = RecordLoader.loadWarc(warcPath, sc)
  }

  test("count records") {
    val warcRecords = RecordLoader.loadWarc(warcPath, sc)
    assert(299L == warcRecords.count)
  }

  test("warc extract domain") {
    val r = RecordLoader.loadWarc(warcPath, sc)
      .keepValidPages()
      .map(r => r.getDomain)
      .countItems()
      .take(10)

    assert(r.length == 3)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}

