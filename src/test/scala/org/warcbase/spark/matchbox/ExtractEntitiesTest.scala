package org.warcbase.spark.matchbox

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.io.{Files, Resources}
import org.apache.commons.io.FileUtils
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.warcbase.spark.matchbox.NER3Classifier.NERClassType

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class ExtractEntitiesTest extends FunSuite with BeforeAndAfter {
  private val LOG = LogFactory.getLog(classOf[ExtractEntitiesTest])
  private val scrapePath = Resources.getResource("ner/example.txt").getPath
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _
  private var tempDir: File = _
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  private val iNerClassfierFile =
    Resources.getResource("ner/classifiers/english.all.3class.distsim.crf.ser.gz").getPath

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
    val expectedEntityMap = mutable.Map[NERClassType.Value, List[String]]()
    expectedEntityMap.put(NERClassType.PERSON, List())
    expectedEntityMap.put(NERClassType.LOCATION, List("Teoma"))
    expectedEntityMap.put(NERClassType.ORGANIZATION, List())
    assert(e._1 == "20080430")
    assert(e._2 == "http://www.archive.org/robots.txt")
    val actual = mapper.readValue(e._3, classOf[Map[String, List[String]]])

    expectedEntityMap.toStream.foreach(f => {
      assert(f._2 == actual.get(f._1.toString).get)
    })
  }

  test("ner3classifier") {
    val rdd = RecordLoader.loadArc(arcPath, sc)
      .map(r => (r.getCrawldate, r.getUrl, r.getRawBodyContent))
    NER3Classifier.setClassifierFile(iNerClassfierFile)
    val entities = rdd.map(r => (r._1, r._2, NER3Classifier.classify(r._3)))
    entities.take(3).foreach(println)
  }

  after {
    FileUtils.deleteDirectory(tempDir)
    LOG.info("Removing tmp files in " + tempDir.getPath)
    if (sc != null) {
      sc.stop()
    }
  }
}
