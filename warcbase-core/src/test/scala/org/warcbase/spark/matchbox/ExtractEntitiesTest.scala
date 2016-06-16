/*
 * Warcbase: an open-source platform for managing web archives
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.warcbase.spark.matchbox

import java.io.File

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.common.io.{Files, Resources}
import org.apache.commons.io.FileUtils
import org.apache.commons.logging.LogFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.warcbase.spark.matchbox.NER3Classifier.NERClassType

import scala.collection.mutable

// There must be a valid classifier file with path `iNerClassifierFile` for this test to pass
// @RunWith(classOf[JUnitRunner])
class ExtractEntitiesTest extends FunSuite with BeforeAndAfter {
  private val LOG = LogFactory.getLog(classOf[ExtractEntitiesTest])
  private val scrapePath = Resources.getResource("ner/example.txt").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _
  private var tempDir: File = _
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private val iNerClassifierFile =
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
    val e = ExtractEntities.extractFromScrapeText(iNerClassifierFile, scrapePath, tempDir + "/scrapeTextEntities", sc).take(3).last
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

  after {
    FileUtils.deleteDirectory(tempDir)
    LOG.info("Removing tmp files in " + tempDir.getPath)
    if (sc != null) {
      sc.stop()
    }
  }
}
