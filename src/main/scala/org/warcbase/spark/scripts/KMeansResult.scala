package org.warcbase.spark.scripts

import org.apache.spark.mllib.clustering.{LDA, KMeansModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vector

/**
  * Created by youngbinkim on 5/15/16.
  */
class KMeansResult(clusters: KMeansModel, tfidf: RDD[Vector], numTopics: Int = 3) {
  implicit class KMeansResult(clusters: KMeansModel) {
    def computeLDA(path: String): Unit = {
      for (i <- 1 to clusters.k) {
        val cluster = tfidf.filter(v => clusters.predict(v) == 1).persist()
        println(s"cluster size ${cluster.count()}")
        val corpus = cluster.zipWithIndex.map(_.swap).cache()
        val ldaModel = new LDA().setK(numTopics).run(corpus)
        println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
        val topics = ldaModel.topicsMatrix
        for (topic <- Range(0, 3)) {
          print("Topic " + topic + ":")
          for (word <- Range(0, ldaModel.vocabSize)) { print(" " + topics(word, topic)); }
          println()
        }
      }
    }
  }
}
