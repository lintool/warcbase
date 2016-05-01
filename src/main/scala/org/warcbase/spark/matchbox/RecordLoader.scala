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
import org.apache.spark.{SerializableWritable, SparkContext}
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.warcbase.io.GenericArchiveRecordWritable.ArchiveFormat
import org.warcbase.io.{GenericArchiveRecordWritable, WarcRecordWritable, ArcRecordWritable}
import org.warcbase.mapreduce.{WacGenericInputFormat, WacWarcInputFormat, WacArcInputFormat}
import org.warcbase.spark.archive.io.{WarcRecord, ArcRecord, ArchiveRecord, GenericArchiveRecord}

object RecordLoader {
  def loadArc(path: String, sc: SparkContext): RDD[ArchiveRecord] = {
    sc.newAPIHadoopFile(path, classOf[WacArcInputFormat], classOf[LongWritable], classOf[ArcRecordWritable])
      .map(r => new ArcRecord(new SerializableWritable(r._2)))
  }

  def loadWarc(path: String, sc: SparkContext): RDD[ArchiveRecord] = {
    sc.newAPIHadoopFile(path, classOf[WacWarcInputFormat], classOf[LongWritable], classOf[WarcRecordWritable])
      .filter(r => r._2.getRecord.getHeader.getHeaderValue("WARC-Type").equals("response"))
      .map(r => new WarcRecord(new SerializableWritable(r._2)))
  }

  def loadArchives(path: String, sc: SparkContext): RDD[ArchiveRecord] = {
    sc.newAPIHadoopFile(path, classOf[WacGenericInputFormat], classOf[LongWritable], classOf[GenericArchiveRecordWritable])
      .filter(r => (r._2.getFormat == ArchiveFormat.ARC) ||
        ((r._2.getFormat == ArchiveFormat.WARC) && r._2.getRecord.getHeader.getHeaderValue("WARC-Type").equals("response")))
      .map(r => new GenericArchiveRecord(new SerializableWritable(r._2)))
  }

  def loadTweets(path: String, sc: SparkContext): RDD[JValue] =
    sc.textFile(path).filter(line => !line.startsWith("{\"delete\":"))
      .map(line => try { parse(line) } catch { case e: Exception => null }).filter(x => x != null)
}
