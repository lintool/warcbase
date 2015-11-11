package org.warcbase.spark.rdd

import com.google.common.io.Resources
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.warcbase.spark.matchbox._
import org.warcbase.spark.rdd.RecordRDD._

@RunWith(classOf[JUnitRunner])
class CountableRDDTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _


  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
  }
  test("count records") {
    val base = RecordLoader.loadArc(arcPath, sc)
      .keepValidPages()
      .map(r => ExtractTopLevelDomain(r.getUrl))
    val r = base
      .map(r => (r, 1))
      .reduceByKey(_ + _)
      .map(_.swap)
      .sortByKey(ascending = false)
      .map(_.swap)
      .collect()
    val r2 = base.countItems().collect()
    assert(r sameElements r2)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}
