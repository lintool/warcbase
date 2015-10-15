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

import scala.reflect.ClassTag

object ArcRecords {
  def load(path: String, sc: SparkContext) = {
    sc.newAPIHadoopFile(path, classOf[WacArcInputFormat], classOf[LongWritable], classOf[ArcRecordWritable])
      .map(r => r._2.getRecord)
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
    } catch {
      case e: Exception => throw new IOException("error parsing url" + url + ". " + e.getMessage)
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

  implicit class ARCRecordRDD[T](rdd: RDD[ARCRecord]) {
    def keepMimeTypes(mimeTypes: Set[String]) = {
      rdd.filter(r => mimeTypes.contains(r.getMetaData.getMimetype))
    }

    def keepDate(date: String) = {
      rdd.filter(r => r.getMetaData.getDate == date)
    }

    def keepUrls(urls: Set[String]) = {
      rdd.filter(r => urls.contains(r.getMetaData.getUrl))
    }

    def discardMimeTypes(mimeTypes: Set[String]) = {
      rdd.filter(r => !mimeTypes.contains(r.getMetaData.getMimetype))
    }

    def discardDate(date: String) = {
      rdd.filter(r => r.getMetaData.getDate != date)
    }

    def discardUrls(urls: Set[String]) = {
      rdd.filter(r => !urls.contains(r.getMetaData.getUrl))
    }

    def extractUrlAndBody() = {
      rdd.map(r => (
        extractTopLevelDomain(r.getMetaData.getUrl).replace("^\\s*www\\.", ""),
        ArcRecordUtils.getBodyContent(r)))
    }
  }

}
