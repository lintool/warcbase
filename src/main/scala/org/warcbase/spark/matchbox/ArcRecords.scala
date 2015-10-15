package org.warcbase.spark.matchbox

import java.io.IOException
import java.net.URL

import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.archive.io.arc.ARCRecord
import org.jsoup.Jsoup
import org.warcbase.data.ArcRecordUtils
import org.warcbase.io.ArcRecordWritable
import org.warcbase.mapreduce.WacArcInputFormat

class ArcRecords {
  protected var rdd: RDD[ARCRecord] = null

  protected var toOutput: RDD[(String, Array[Byte])] = null

  def load(path: String, sc: SparkContext) = {
    rdd = sc.newAPIHadoopFile(path, classOf[WacArcInputFormat], classOf[LongWritable], classOf[ArcRecordWritable])
      .map(r => r._2.getRecord)
    this
  }

  def keepMimeTypes(mimeTypes: Seq[String]) = {
    mimeTypes.foreach(mt => rdd = rdd.filter(r => r.getMetaData.getMimetype == mt))
    this
  }

  def keepDate(date: String) = {
    rdd = rdd.filter(r => r.getMetaData.getDate == date)
    this
  }

  def keepUrl(urls: Seq[String]) = {
    urls.foreach(url =>
      rdd = rdd.filter(r =>
        extractTopLevelDomain(r.getMetaData.getUrl).replace("^\\s*www\\.", "") == url
      )
    )
    this
  }

  def discardMimeTypes(mimeTypes: Seq[String]) = {
    mimeTypes.foreach(mt => rdd = rdd.filter(r => r.getMetaData.getMimetype != mt))
    this
  }

  def discardDate(date: String) = {
    rdd = rdd.filter(r => r.getMetaData.getDate != date)
    this
  }

  def discardUrl(urls: Seq[String]) = {
    urls.foreach(url => rdd = rdd.filter(r => r.getMetaData.getUrl != url))
    this
  }

  def extractUrlAndBody() = {
    toOutput = rdd.map(r => (
      extractTopLevelDomain(r.getMetaData.getUrl).replace("^\\s*www\\.", ""),
      ArcRecordUtils.getBodyContent(r)))
    this
  }

  def output(outputPath: String) = {
    toOutput.saveAsTextFile(outputPath)
  }

  def extractTopLevelDomain(url: String, source: String = ""): String = {
    if (url == null) return null
    try {
      val u = new URL(url)
      if (u.getHost == null) {
        val s = new URL(source)
        s.getHost
      } else {
        u.getHost
      }
    }
  }

  def extractRawText(content: String) = {
    try {
      Jsoup.parse(content).text().replaceAll("[\\r\\n]+", " ")
    }
    catch {
      case e: Exception => throw new IOException("Caught exception processing input row ", e)
    }
  }
}
