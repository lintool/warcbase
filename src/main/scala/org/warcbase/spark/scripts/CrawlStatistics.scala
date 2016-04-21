package org.warcbase.spark.scripts

import com.google.common.io.Resources
import org.apache.spark.{SparkConf, SparkContext}
import org.warcbase.spark.matchbox.{ExtractLinks, ExtractDomain, RecordLoader}
import org.warcbase.spark.matchbox._
import org.warcbase.spark.matchbox.StringUtils._
import org.warcbase.spark.rdd.RecordRDD._

object CrawlStatistics {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val warcPath = Resources.getResource("warc/example.warc.gz").getPath

  def numPagesPerCrawl(sc: SparkContext) = {
    val a = RecordLoader.loadArc(arcPath, sc)
    val b = RecordLoader.loadWarc(warcPath, sc)
    val pageCounts = a.union(b)
      .keepValidPages()
      .map(_.getCrawlDate)
      .countItems()
    println(pageCounts.take(1).mkString)
  }

  def numLinksPerCrawl(sc: SparkContext) = {
    val a = RecordLoader.loadArc(arcPath, sc)
    val b = RecordLoader.loadWarc(warcPath, sc)
    val linkCounts = a.union(b)
      .keepValidPages()
      .map(r => (r.getCrawlDate.substring(0, 6), ExtractLinks(r.getUrl, r.getContentString)))
      .flatMap(r => r._2.map(f => (r._1, f._1, f._2)))
      .filter(r => r._2 != null && r._3 != null)
      .map(_._1)
      .countItems()
    println(linkCounts.take(1).mkString)
  }

  def numPagesByDomain(sc: SparkContext): Unit = {
    val a = RecordLoader.loadArc(arcPath, sc)
    val b = RecordLoader.loadWarc(warcPath, sc)
    val domainCounts = a.union(b)
      .keepValidPages()
      .map(r => (r.getCrawlDate.substring(0, 6), r.getDomain.removePrefixWWW()))
      .countItems()
      .filter(f => f._2 > 10)
      .sortBy(f => (f._1, f._2))
      .collect()
    println(domainCounts.take(1).mkString)
  }

  def linkStructure(sc: SparkContext) = {
    val linkStructure = RecordLoader.loadArc(arcPath, sc)
      .keepValidPages()
      .map(r => (r.getCrawlDate.substring(0, 6), ExtractLinks(r.getUrl, r.getContentString)))
      .flatMap(r => r._2.map(f => (r._1, ExtractDomain(f._1).removePrefixWWW(), ExtractDomain(f._2).removePrefixWWW())))
      .filter(r => r._2 != null && r._3 != null)
      .countItems()
      .collect()
    println(linkStructure.take(1).mkString)
  }

  def warclinkStructure(sc: SparkContext) = {
    val linkStructure = RecordLoader.loadWarc(warcPath, sc)
      .keepValidPages()
      .map(r => (ExtractDate(r.getCrawlDate, ExtractDate.DateComponent.YYYYMM), ExtractLinks(r.getUrl, r.getContentString)))
      .flatMap(r => r._2.map(f => (r._1, ExtractDomain(f._1).removePrefixWWW(), ExtractDomain(f._2).removePrefixWWW())))
      .filter(r => r._2 != null && r._3 != null)
      .countItems()
      .map(r => TupleFormatter.tabDelimit(r))
      .collect()
    println(linkStructure.take(1).head)
  }

  def main(args: Array[String]) = {
    val master = "local[4]"
    val appName = "example-spark"
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
    val sc = new SparkContext(conf)
    try {
      numPagesPerCrawl(sc)
      numLinksPerCrawl(sc)
      numPagesByDomain(sc)
      linkStructure(sc)
      warclinkStructure(sc)
    } finally {
      sc.stop()
    }
  }
}
