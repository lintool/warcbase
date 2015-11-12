package org.warcbase.spark.matchbox

import java.io.File

import com.google.common.io.{Files, Resources}
import org.apache.commons.io.FileUtils
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}

@RunWith(classOf[JUnitRunner])
class ExtractEntitiesTest extends FunSuite with BeforeAndAfter {
  private val LOG = LogFactory.getLog(classOf[ExtractEntitiesTest])
  private val scrapePath = Resources.getResource("ner/example.txt").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _
  private var tempDir: File = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
    tempDir = Files.createTempDir()
    LOG.info("Output can be found in " + tempDir.getPath)
  }

  test("extract entities") {
    val e = ExtractEntities.extractFromScrapeText(scrapePath, tempDir + "/scrapeTextEntities", sc).take(3).last
    assert(e._1 == "20080430")
    assert(e._2 == "http://www.archive.org/robots.txt")
    assert(e._3 == "{PERSON=[]ORGANIZATION=[]LOCATION=[Teoma]O=[]}")
  }

  after {
    FileUtils.deleteDirectory(tempDir)
    LOG.info("Removing tmp files in " + tempDir.getPath)
    if (sc != null) {
      sc.stop()
    }
  }
}
