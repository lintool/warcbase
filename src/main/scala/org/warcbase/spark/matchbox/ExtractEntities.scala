package org.warcbase.spark.matchbox

import com.google.common.io.Resources
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Extracts entities
  */
object ExtractEntities {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _
  private val iNerClassfierFile =
    Resources.getResource("ner/classifiers/english.all.3class.distsim.crf.ser.gz").getPath

  /**
    * @param inputRecordFile path of ARC or WARC file from which to extract entities
    * @param outputFile path of output directory
    */
  def extractFromRecords(inputRecordFile: String, outputFile: String, sc: SparkContext): RDD[(String, String, String)] = {
    val rdd = RecordLoader.loadArc(inputRecordFile, sc)
      .map(r => (r.getCrawldate, r.getUrl, r.getRawBodyContent))
    extractAndOutput(rdd, outputFile)
  }

  /**
    * @param inputFile path of file with tuples (date: String, url: String, content: String)
    *                  from which to extract entities
    * @param outputFile path of output directory
    */
  def extractFromScrapeText(inputFile: String, outputFile: String, sc: SparkContext): RDD[(String, String, String)] = {
    val rdd = sc.textFile(inputFile)
      .map(line => {
        val ind1 = line.indexOf(",")
        val ind2 = line.indexOf(",", ind1 + 1)
        (line.substring(1, ind1),
          line.substring(ind1 + 1, ind2),
          line.substring(ind2 + 1, line.length - 1))
      })
    extractAndOutput(rdd, outputFile)
  }

  /**
    * @param rdd with values (date, url, content)
    * @param outputFile path of output directory
    */
  def extractAndOutput(rdd: RDD[(String, String, String)], outputFile: String): RDD[(String, String, String)] = {
    NER3Classifier.apply(iNerClassfierFile)
    val r = rdd.map(r => (r._1, r._2, NER3Classifier.classify(r._3)))
    r.saveAsTextFile(outputFile)
    r
  }
}
