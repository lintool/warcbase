package org.warcbase.spark.scripts

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.{LDA, KMeansModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector


class KMeansResult(clusters: KMeansModel, tfidf: RDD[Vector], lemmatized: RDD[Seq[String]]) {
  lazy val indexToTerm = lemmatized.flatMap(seq => seq.map(f=>f)).map(s=>(hashingTF.indexOf(s), s)).cache()
  val hashingTF = new HashingTF()
  
  def computeLDA(output: String, numTopics: Int = 3): Unit = {
    for (i <- 1 to clusters.k) {
      val cluster = tfidf.filter(v => clusters.predict(v) == 1).persist()
      println(s"cluster size ${cluster.count()}")
      val corpus = cluster.zipWithIndex.map(_.swap).cache()
      val ldaModel = new LDA().setK(numTopics).run(corpus)
      println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
      val topics = ldaModel.topicsMatrix
      for (topic <- Range(0, numTopics)) {
        print("Topic " + topic + ":")
        for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
        println()
      }
    }
  }

  def topNWords(output: String, limit: Int = 10, sc: SparkContext) = {
    clusters.clusterCenters.foreach(v => {
      val topWords = sc.parallelize(v.toArray).zipWithIndex.takeOrdered(limit)(Ordering[Double].reverse.on(x=>x._1));
      sc.parallelize(topWords).map{ case (k, i) => (k, indexToTerm.lookup(i.toInt))}.saveAsTextFile(output)
    })
  }
}
