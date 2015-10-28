package org.warcbase.spark.matchbox

import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.archive.io.arc.ARCRecord
import org.warcbase.data.ArcRecordUtils
import org.warcbase.io.ArcRecordWritable
import org.warcbase.mapreduce.WacArcInputFormat

// Helper functions on RDD[ArcRecord]
object ArcRecords extends java.io.Serializable {
  def load(path: String, sc: SparkContext) = {
    sc.newAPIHadoopFile(path, classOf[WacArcInputFormat], classOf[LongWritable], classOf[ArcRecordWritable])
      .map(r => r._2.getRecord)
  }

  implicit class ARCRecordRDD(rdd: RDD[ARCRecord]) extends java.io.Serializable {
    def keepMimeTypes(mimeTypes: Set[String]) = {
      rdd.filter(r => mimeTypes.contains(r.getMetaData.getMimetype))
    }

    def keepDate(date: String) = {
      rdd.filter(r => r.getMetaData.getDate == date)
    }

    def keepUrls(urls: Set[String]) = {
      rdd.filter(r => urls.contains(r.getMetaData.getUrl))
    }

    def keepDomains(urls: Set[String]) = {
      rdd.filter(r => urls.contains(ExtractTopLevelDomain(r.getMetaData.getUrl).replace("^\\s*www\\.", "")))
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

    def discardDomains(urls: Set[String]) = {
      rdd.filter(r => !urls.contains(ExtractTopLevelDomain(r.getMetaData.getUrl).replace("^\\s*www\\.", "")))
    }

    def extractDomainUrlBody() = {
      rdd.map(r => (
        ExtractTopLevelDomain(r.getMetaData.getUrl).replace("^\\s*www\\.", ""),
        r.getMetaData.getUrl,
        ExtractRawText(new String(ArcRecordUtils.getBodyContent(r)))
        )
      )
    }

    def extractCrawldateDomainUrlBody() = {
      rdd.map(r => (
        r.getMetaData.getDate.substring(0, 8),
        ExtractTopLevelDomain(r.getMetaData.getUrl).replace("^\\s*www\\.", ""),
        r.getMetaData.getUrl,
        ExtractRawText(new String(ArcRecordUtils.getBodyContent(r)))
        )
      )
    }
  }

}
