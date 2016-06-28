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
import org.warcbase.spark.matchbox._

@RunWith(classOf[JUnitRunner])
class GenericArchiveRecordTest extends FunSuite with BeforeAndAfter {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val warcPath = Resources.getResource("warc/example.warc.gz").getPath
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
    assert(RecordLoader.loadArchives(arcPath, sc).count == 300L)
    assert(RecordLoader.loadArchives(warcPath, sc).count == 299L)
  }

  test("warc and arc get content") {
    val arc10 = RecordLoader.loadArc(arcPath, sc)
      .map(r => r.getContentString)
      .take(10)
    var archives10 = RecordLoader.loadArchives(arcPath, sc)
      .map(r => r.getContentString)
      .take(10)
    for(i <- 0 to 9) { assert(arc10(i) == archives10(i)) }

   val warc10 = RecordLoader.loadWarc(warcPath, sc)
      .map(r => r.getContentString)
      .take(10)
    archives10 = RecordLoader.loadArchives(warcPath, sc)
      .map(r => r.getContentString)
      .take(10)
    for(i <- 0 to 9) { assert(warc10(i) == archives10(i)) }
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}

