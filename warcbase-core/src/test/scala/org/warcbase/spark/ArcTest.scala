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

package org.warcbase.spark

import com.google.common.io.Resources
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.warcbase.spark.matchbox.ExtractDate.DateComponent
import org.warcbase.spark.matchbox._
import org.warcbase.spark.rdd.RecordRDD._

@RunWith(classOf[JUnitRunner])
class ArcTest extends FunSuite with BeforeAndAfter {
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
    assert(RecordLoader.loadArc(arcPath, sc).count == 300L)
  }

  test("filter date") {
    val four = RecordLoader.loadArc(arcPath, sc)
      .keepDate("200804", DateComponent.YYYYMM)
      .collect()
    val five = RecordLoader.loadArc(arcPath, sc)
      .keepDate("200805", DateComponent.YYYYMM)
      .collect()
    four.foreach(r => assert(r.getCrawlDate.substring(0, 6) == "200804"))
    five.foreach(r => assert(r.getCrawlDate.substring(0, 6) == "200805"))
  }

  test("filter url pattern") {
    val keepMatches = RecordLoader.loadArc(arcPath, sc)
      .keepUrlPatterns(Set("http://www.archive.org/about/.*".r))
    val discardMatches = RecordLoader.loadArc(arcPath, sc)
        .discardUrlPatterns(Set("http://www.archive.org/about/.*".r))
    assert(keepMatches.count == 16L)
    assert(discardMatches.count == 284L)
  }

  test("count links") {
    val links = RecordLoader.loadArc(arcPath, sc)
      .map(r => ExtractLinks(r.getUrl, r.getContentString))
      .reduce((a, b) => a ++ b)
    assert(links.size == 664)
  }

  test("detect language") {
    val languageCounts = RecordLoader.loadArc(arcPath, sc)
      .keepMimeTypes(Set("text/html"))
      .map(r => RemoveHTML(r.getContentString))
      .groupBy(content => DetectLanguage(content))
      .map(f => {
        (f._1, f._2.size)
      })
      .collect

    languageCounts.foreach {
      case ("en", count) => assert(57L == count)
      case ("et", count) => assert(6L == count)
      case ("it", count) => assert(1L == count)
      case ("lt", count) => assert(61L == count)
      case ("no", count) => assert(6L == count)
      case ("ro", count) => assert(4L == count)
      case (_, count) => print(_)
    }
  }

  test("detect mime type tika") {
    val mimeTypeCounts = RecordLoader.loadArc(arcPath, sc)
      .map(r => RemoveHTML(r.getContentString))
      .groupBy(content => DetectMimeTypeTika(content))
      .map(f => {
        println(f._1 + " : " + f._2.size)
        (f._1, f._2.size)
      }).collect

    mimeTypeCounts.foreach {
      case ("image/gif", count) => assert(29L == count)
      case ("image/png", count) => assert(8L == count)
      case ("image/jpeg", count) => assert(18L == count)
      case ("text/html", count) => assert(132L == count)
      case ("text/plain", count) => assert(229L == count)
      case ("application/xml", count) => assert(1L == count)
      case ("application/rss+xml", count) => assert(9L == count)
      case ("application/xhtml+xml", count) => assert(1L == count)
      case ("application/octet-stream", count) => assert(26L == count)
      case ("application/x-shockwave-flash", count) => assert(8L == count)
      case (_, count) => print(_)
    }
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}

