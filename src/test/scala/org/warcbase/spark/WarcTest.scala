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
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite}
import org.warcbase.spark.archive.io.ArchiveRecord
import org.warcbase.spark.matchbox.RecordLoader
import org.warcbase.spark.rdd.RecordRDD._

@RunWith(classOf[JUnitRunner])
class WarcTest extends FunSuite with BeforeAndAfter {

  private val warcPath = Resources.getResource("warc/example.warc.gz").getPath
  private val master = "local[2]"
  private val appName = "example-spark"
  private var sc: SparkContext = _
  private var records: RDD[ArchiveRecord] = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    sc = new SparkContext(conf)
    records = RecordLoader.loadWarc(warcPath, sc)
  }

  test("count records") {
    assert(299L == records.count)
  }

  test("warc extract domain") {
    val r = records
      .keepValidPages()
      .map(r => r.getDomain)
      .countItems()
      .take(10)

    assert(r.length == 3)
  }

  test("warc get content") {
    val a = RecordLoader.loadWarc(warcPath, sc)
      .map(r => r.getContentString)
      .take(1)
    assert(a.head.nonEmpty)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}

