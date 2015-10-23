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

    val functions = Map(
      (ToExtract.CRAWLDATE, (r: WARecord) => r.getCrawldate),
      (ToExtract.DOMAIN, (r: WARecord) => r.getDomain),
      (ToExtract.URL, (r: WARecord) => r.getUrl),
      (ToExtract.BODY, (r: WARecord) => ExtractRawText(r.getBodyContent))
    )

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
      val list = params.map { p => functions.get(p).get }
      rdd.map(r => {
        if (list.size == 1) {
          list.head(r)
        } else if (list.size == 2) {
          (list.head(r), list(1)(r))
        } else if (list.size == 3) {
          (list.head(r), list(1), list(2))
        } else if (list.size == 4) {
          (list.head(r), list(1)(r), list(2)(r), list(3)(r))
        } else {
          throw sys.error("no matching parameters")
        }
      })
    }
  }

}
