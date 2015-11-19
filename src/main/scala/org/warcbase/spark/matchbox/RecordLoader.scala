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
