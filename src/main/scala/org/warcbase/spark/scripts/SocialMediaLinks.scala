package org.warcbase.spark.scripts

import org.apache.spark.SparkContext
import org.warcbase.spark.matchbox.{ExtractLinks, RecordLoader}
import org.warcbase.spark.rdd.RecordRDD._

object SocialMediaLinks {

  def socialMediaLinksCount(sc: SparkContext) = {
    RecordLoader.loadArc("/shared/collections/CanadianPoliticalParties/arc/", sc)
      .map(r => (r.getCrawldate, r.getDomain, ExtractLinks(r.getUrl, r.getBodyContent)))
      .flatMap(r => r._3
        .filter(f => f._2.matches(".*(twitter|facebook|youtube).*"))
        .map(f => (r._1, r._2, f._2)))
      .countItems()
      .saveAsTextFile("cpp.socialmedia/")
  }
}
