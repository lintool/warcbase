package org.warcbase.spark.matchbox

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Extracts entities
  */
object ExtractEntities {

  /**
    * @param iNerClassifierFile path of classifier file
    * @param inputRecordFile path of ARC or WARC file from which to extract entities
    * @param outputFile path of output directory
    */
  def extractFromRecords(iNerClassifierFile: String, inputRecordFile: String, outputFile: String, sc: SparkContext): RDD[(String, String, String)] = {
    val rdd = RecordLoader.loadArc(inputRecordFile, sc)
      .map(r => (r.getCrawldate, r.getUrl, r.getRawBodyContent))
    extractAndOutput(iNerClassifierFile, rdd, outputFile)
  }

  /**
    * @param iNerClassifierFile path of classifier file
    * @param inputFile path of file with tuples (date: String, url: String, content: String)
    *                  from which to extract entities
    * @param outputFile path of output directory
    */
  def extractFromScrapeText(iNerClassifierFile: String, inputFile: String, outputFile: String, sc: SparkContext): RDD[(String, String, String)] = {
    val rdd = sc.textFile(inputFile)
      .map(line => {
        val ind1 = line.indexOf(",")
        val ind2 = line.indexOf(",", ind1 + 1)
        (line.substring(1, ind1),
          line.substring(ind1 + 1, ind2),
          line.substring(ind2 + 1, line.length - 1))
      })
    extractAndOutput(iNerClassifierFile, rdd, outputFile)
  }

  /**
    * @param iNerClassifierFile path of classifier file
    * @param rdd with values (date, url, content)
    * @param outputFile path of output directory
    */
  def extractAndOutput(iNerClassifierFile: String, rdd: RDD[(String, String, String)], outputFile: String): RDD[(String, String, String)] = {
    val r = rdd.mapPartitions(iter => {
      NER3Classifier.apply(iNerClassifierFile)
      iter.map(r => (r._1, r._2, NER3Classifier.classify(r._3)))
    })
    r.saveAsTextFile(outputFile)
    r
  }
}
