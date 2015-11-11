package org.warcbase.spark.scripts

import com.google.common.io.Resources
import org.apache.spark.{SparkConf, SparkContext}
import org.warcbase.spark.matchbox.{RecordLoader, NER3Classifier}

/**
  * Extracts entities from records
  */
object ExtractEntitiesFromScrapeText {
  private val arcPath = Resources.getResource("arc/example.arc.gz").getPath
  private val master = "local[4]"
  private val appName = "example-spark"
  private var sc: SparkContext = _
  private val iNerClassfierFile =
    Resources.getResource("ner/classifiers/english.all.3class.distsim.crf.ser.gz").getPath

  def apply(input: String, output: String, sc: SparkContext) = {
    NER3Classifier.apply(iNerClassfierFile)
    RecordLoader.loadArc(input, sc)
      .map(r => (r.getCrawldate, r.getUrl, r.getRawBodyContent))
      .map(r => (r._1, r._2, NER3Classifier.classify(r._3)))
      .saveAsTextFile(output)
  }

  def main(args: Array[String]): Unit = {

    try {
      val conf = new SparkConf()
        .setMaster(master)
        .setAppName(appName)
      sc = new SparkContext(conf)

      ExtractEntitiesFromScrapeText(arcPath,
        "entities",
        sc)

    } finally {
      sc.stop()
    }
  }
}
