package org.warcbase.spark.scripts

import org.apache.spark.mllib.clustering.{KMeansModel, LDA}
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkContext}

import scala.collection.mutable.ArrayBuffer


class KMeansArchiveCluster(clusters: KMeansModel, tfidf: RDD[Vector], lemmatized: RDD[Seq[String]],
                           rec: RDD[String]) extends Serializable{
  val hashingTF = new HashingTF()
  lazy val allWords = lemmatized.flatMap(seq => seq.map(f=>f)).persist()
  lazy val hashIndexToTerm = allWords.map(s=>(hashingTF.indexOf(s), s)).distinct().cache()
  lazy val clusterRdds = getClusterRdds()

  private def getClusterRdds() = {
    val rdds = new ArrayBuffer[(Int, RDD[(Vector, String)])]
    val merged = tfidf.zip(rec).map(r=>(r._1, r._2))
    for (i <- 0 to clusters.k-1) {
      rdds += Pair(i, merged.filter(v => clusters.predict(v._1) == i).persist())
    }
    rdds
  }

  def getSampleDocs(sc: SparkContext, numDocs: Int=10): RDD[(Int, String)] ={
    var res:RDD[(Int, String)] = sc.emptyRDD[(Int, String)]
    clusterRdds.par.foreach(c => {
      val cluster = c._2
      val p = clusters.clusterCenters(c._1)
      val docs = cluster.map(r => (Vectors.sqdist(p, r._1), r._2)).takeOrdered(numDocs)(Ordering[Double].on(x => x._1))
      res = res.union(sc.parallelize(docs).map(r => (c._1, r._2)))
    })
    res
  }

  def saveSampleDocs(output: String, sc: SparkContext) = {
    getSampleDocs(sc).partitionBy(new HashPartitioner(clusters.k)).map(r=>r._2).saveAsTextFile(output)
    this
  }

  def computeLDA(output: String, sc: SparkContext, numTopics: Int = 3, numWordsPerTopic: Int = 10) = {
    var res:RDD[(Int, (Long, Seq[String], Double))] = sc.emptyRDD[(Int, (Long, Seq[String], Double))]
    clusterRdds.par.foreach(c => {
      val cluster = c._2.map(x=>x._1).persist()
      println(s"cluster size ${cluster.count()}")
      val corpus = cluster.zipWithIndex.map(_.swap).cache()
      val ldaModel = new LDA().setK(numTopics).run(corpus)
      val topicArr:Array[(Array[Int], Array[Double])] = ldaModel.describeTopics(numWordsPerTopic)
      val topicRdd:RDD[(Array[Seq[String]], Array[Double])] = sc.parallelize(
        topicArr.map(topic => (topic._1.map(index => hashIndexToTerm.lookup(index)), topic._2))).cache()
      val topicWords = topicRdd.zipWithIndex().map(_.swap).flatMap(r => r._2._1.map(word => (r._1, word)))
      val topicScores = topicRdd.flatMap(r => r._2.map(word=>word))
      val topics = topicWords.zip(topicScores)
      res = res.union(topics.map(r => (c._1, (r._1._1, r._1._2, r._2))))
    })
    res.partitionBy(new HashPartitioner(clusters.k)).map(r=>r._2).saveAsTextFile(output)
    this
  }

  def topNWords(output: String, sc: SparkContext, limit: Int = 10) = {
    var res:RDD[(Int, (Double, Seq[String]))] = sc.emptyRDD[(Int, (Double, Seq[String]))]
    clusterRdds.par.foreach(c => {
      val v = c._1
      val cluster = clusters.clusterCenters(v)
      val topWords = sc.parallelize(cluster.toArray).zipWithIndex.takeOrdered(limit)(Ordering[Double].reverse.on(x=>x._1));
      res = res.union(sc.parallelize(topWords.map{ case (k, i) => (v, (k, hashIndexToTerm.lookup(i.toInt)))}))
    })
    res.partitionBy(new HashPartitioner(clusters.k)).map(r=>r._2).saveAsTextFile(output)
    this
  }
}
