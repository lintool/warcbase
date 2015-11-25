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

import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.warcbase.io.{WarcRecordWritable, ArcRecordWritable}
import org.warcbase.mapreduce.{WacWarcInputFormat, WacArcInputFormat}
import org.warcbase.spark.matchbox.RecordTransformers.WARecord

object RecordLoader {
  def loadArc(path: String, sc: SparkContext): RDD[WARecord] = {
    sc.newAPIHadoopFile(path, classOf[WacArcInputFormat], classOf[LongWritable], classOf[ArcRecordWritable])
      .map(r => r._2.getRecord)
  }

  def loadWarc(path: String, sc: SparkContext): RDD[WARecord] = {
    sc.newAPIHadoopFile(path, classOf[WacWarcInputFormat], classOf[LongWritable], classOf[WarcRecordWritable])
      .filter(r => r._2.getRecord.getHeader.getHeaderValue("WARC-Type").equals("response"))
      .map(r => r._2.getRecord)
  }
}
