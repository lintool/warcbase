package org.warcbase.spark.matchbox

import org.apache.spark.{RangePartitioner, SparkContext}
import org.warcbase.spark.rdd.RecordRDD._
import org.apache.spark.rdd.RDD
import org.warcbase.spark.archive.io.ArchiveRecord


/**
  * Extract most popular images
  *
  * limit: number of most popular images in the output
  * timeoutVal: time allowed to connect to each image
  */
object ExtractPopularImages {
  def apply(records: RDD[ArchiveRecord], limit: Int, sc:SparkContext, minWidth: Int = 30, minHeight: Int = 30) = {
    val res = records
      .keepImages()
      .map(r => ((r.getUrl, r.getImageBytes), 1))
      .map(img => (ComputeMD5(img._1._2), (ComputeImageSize(img._1._2), img._1._1, img._2)))
      .filter(img => img._2._1._1 >= minWidth && img._2._1._2 >= minHeight)
      .reduceByKey((image1, image2) => (image1._1, image1._2, image1._3 + image2._3))
      .map(x=> (x._2._3, x._2._2))
      .takeOrdered(limit)(Ordering[Int].on(x => -x._1))
    res.foreach(x => println(x._1 + "\t" + x._2))
    val numPartitions = if (limit <= 500) 1 else Math.ceil(limit / 250).toInt
    val rdd = sc.parallelize(res)
    rdd.repartitionAndSortWithinPartitions(
      new RangePartitioner(numPartitions, rdd, false)).sortByKey(false).map(x=>x._1 + "\t" + x._2)
  }
}
