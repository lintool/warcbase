package org.warcbase.spark.matchbox

import org.apache.hadoop.io.LongWritable
import org.jsoup.Jsoup
import org.jsoup.nodes.{Element, Document}
import org.jsoup.select.Elements
import org.warcbase.data.ArcRecordUtils
import org.warcbase.io.ArcRecordWritable

import scala.collection.mutable

class ExtractLinks {
  def map(k: LongWritable, r: ArcRecordWritable) = {
    val record = r.getRecord
    val meta = record.getMetaData
    val url = meta.getUrl

    val bytes: Array[Byte] = ArcRecordUtils.getBodyContent(record)
    val doc: Document = Jsoup.parse(new String(bytes, "UTF8"), url)
    val links: Elements = doc.select("a[href]")

    val linkUrlSet = mutable.Set[String]()

    for (link: Element <- links) {
      val linkUrl = link.attr("abs:href")
      linkUrlSet.add(linkUrl)
    }
    linkUrlSet.seq
  }
}
