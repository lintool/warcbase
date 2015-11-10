package org.warcbase.spark.rdd

import org.apache.spark.rdd.RDD
import org.warcbase.spark.matchbox.RecordTransformers.WARecord
import org.warcbase.spark.matchbox.{ExtractRawText, ExtractTopLevelDomain}

/**
  * A Wrapper class around RDD to allow RDDs of type ARCRecord and WARCRecord to be queried via a fluent API.
  *
  * To load such an RDD, please see [[org.warcbase.spark.matchbox.RecordLoader]]
  */
object RecordRDD extends java.io.Serializable {

  object ToExtract extends Enumeration {
    val CRAWLDATE = Value("CRAWLDATE")
    val DOMAIN = Value("DOMAIN")
    val URL = Value("URL")
    val BODY = Value("BODY")
  }

  implicit class WARecordRDD(rdd: RDD[WARecord]) extends java.io.Serializable {

    def keepMimeTypes(mimeTypes: Set[String]) = {
      rdd.filter(r => mimeTypes.contains(r.getMimeType))
    }

    def keepDate(date: String) = {
      rdd.filter(r => r.getCrawldate == date)
    }

    def keepUrls(urls: Set[String]) = {
      rdd.filter(r => urls.contains(r.getUrl))
    }

    def keepDomains(urls: Set[String]) = {
      rdd.filter(r => urls.contains(ExtractTopLevelDomain(r.getUrl).replace("^\\s*www\\.", "")))
    }

    def discardMimeTypes(mimeTypes: Set[String]) = {
      rdd.filter(r => !mimeTypes.contains(r.getMimeType))
    }

    def discardDate(date: String) = {
      rdd.filter(r => r.getCrawldate != date)
    }

    def discardUrls(urls: Set[String]) = {
      rdd.filter(r => !urls.contains(r.getUrl))
    }

    def discardDomains(urls: Set[String]) = {
      rdd.filter(r => !urls.contains(r.getDomain))
    }

    def extract(params: ToExtract.Value*) = {
      val func = params.map {
        case ToExtract.CRAWLDATE => (r: WARecord) => r.getCrawldate
        case ToExtract.DOMAIN => (r: WARecord) => r.getDomain
        case ToExtract.URL => (r: WARecord) => r.getUrl
        case ToExtract.BODY => (r: WARecord) => ExtractRawText(r.getBodyContent)
      }
      rdd.map(r => {
        func.size match {
          case 1 => func.head(r)
          case 2 => (func.head(r), func(1)(r))
          case 3 => (func.head(r), func(1)(r), func(2)(r))
          case 4 => (func.head(r), func(1)(r), func(2)(r), func(3)(r))
          case _ => throw sys.error("no matching parameters")
        }
      })
    }
  }
}
