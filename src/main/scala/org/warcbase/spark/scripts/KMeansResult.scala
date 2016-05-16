package org.warcbase.spark.scripts

import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.mllib.clustering.{LDA, KMeansModel}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.warcbase.spark.archive.io.ArchiveRecord

import scala.collection.mutable.ArrayBuffer


class KMeansResult(clusters: KMeansModel, tfidf: RDD[Vector], lemmatized: RDD[Seq[String]], sc: SparkContext,
                   rec: RDD[ArchiveRecord]) {
  val hashingTF = new HashingTF()
  lazy val allWords = lemmatized.flatMap(seq => seq.map(f=>f)).persist()
  lazy val hashIndexToTerm = allWords.map(s=>(hashingTF.indexOf(s), s)).distinct().cache()
  lazy val indexToTerm = allWords.zipWithIndex().map(s=>(s._2, s._1)).cache()
  lazy val clusterRdds = getClusterRdds()

  private def getClusterRdds() = {
    val rdds = new ArrayBuffer[RDD[(Vector, String)]]
    val merged = tfidf.zip(rec).map(r=>(r._1, r._2.getContentString))
    for (i <- 0 to clusters.k-1) {
      rdds += merged.filter(v => clusters.predict(v._1) == i).persist()
    }
    rdds
  }

  def getSampleDocs(numDocs: Int=10): RDD[(Int, String)] ={
    val sampleDocs = new ArrayBuffer[(Int, String)]
    for (i <- 0 to clusters.k-1) {
      val cluster = clusterRdds(i)
      val p= clusters.clusterCenters(i)
      val docs = cluster.map(r=> (Vectors.sqdist(p, r._1), r._2)).takeOrdered(numDocs)(Ordering[Double].on(x=>x._1))
      docs.foreach(doc => {
        sampleDocs += Tuple2(i, doc._2)
      })
    }
    sc.parallelize(sampleDocs)
  }

  def saveSampleDocs(output: String) = {
    getSampleDocs().partitionBy(new HashPartitioner(clusters.k)).saveAsTextFile(output)
  }

  def computeLDA(output: String, numTopics: Int = 3) = {
    for (i <- 0 to clusters.k-1) {
      val cluster = tfidf.filter(v => clusters.predict(v) == i).persist()
      println(s"cluster size ${cluster.count()}")
      val corpus = cluster.zipWithIndex.map(_.swap).cache()
      val ldaModel = new LDA().setK(numTopics).run(corpus)
      println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize + " words):")
      val topics = ldaModel.topicsMatrix
      val topWords = sc.parallelize(topics.toArray).zipWithIndex.takeOrdered(15)(Ordering[Double].reverse.on(x=>x._1))
      val topWordsTuples = topWords.map{ case (k, v) => (k, indexToTerm.lookup((v / numTopics).toInt), v % numTopics, (v / numTopics).toInt)}
      val res = topWordsTuples.sortBy(tuple => tuple._3)
      res.foreach(println)

      for (topic <- Range(0, numTopics)) {
        print("Topic " + topic + ":")
        for (word <- Range(0, 30)) { print(" " + word + " . topic : " + topic + " " + topics(word, topic)); }

        println()
      }
    }
  }

  def topNWords(output: String, limit: Int = 10) = {
    clusters.clusterCenters.foreach(v => {
      val topWords = sc.parallelize(v.toArray).zipWithIndex.takeOrdered(limit)(Ordering[Double].reverse.on(x=>x._1));
      val res = topWords.map{ case (k, i) => (k, hashIndexToTerm.lookup(i.toInt))}
      res.foreach{
        case(k, arr) =>
      }
      println
      // sc.parallelize(res).saveAsTextFile(output)
    })
    this
  }
}
