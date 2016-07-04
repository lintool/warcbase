package org.warcbase.spark.matchbox

import org.apache.spark.{HashPartitioner, AccumulatorParam, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.jsoup.Jsoup
import org.tartarus.snowball.ext.EnglishStemmer
import org.warcbase.spark.archive.io.ArchiveRecord
import org.warcbase.spark.rdd.RecordRDD._
import org.warcbase.spark.scripts.KMeansArchiveCluster
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object ExtractClusters {
  implicit object MapAccumulator extends AccumulatorParam[Map[Int, Double]] {
    def zero(m: Map[Int, Double]) = Map()
    def addInPlace(m1: Map[Int, Double], m2: Map[Int, Double]) = m1 ++ m2
  }

  def runTestK(tfidf: RDD[Vector], maxIterations: Int, maxK: Int, sc: SparkContext, stepK: Int, minK: Int) = {
    val ran = Range(minK, maxK, stepK)
    val accum = sc.accumulator(Map[Int, Double]())(MapAccumulator)
    ran.par.foreach(i => {
      val model = KMeans.train(tfidf, i, maxIterations)
      val cost =model.computeCost(tfidf)
      accum += Map((i, cost))
    })
    accum.value.toSeq.sortBy(_._1).foreach(pair=>{
      println(s"k: ${pair._1}\tcost:${pair._2}")
    })

    // val x = minK.toDouble until maxK.toDouble by stepK.toDouble
    // output(ASCII, xyChart(x -> accum.value.toSeq.sortBy(_._1).map(_._2)))
  }

  implicit object ArrayAccumulator extends AccumulatorParam[ArrayBuffer[(Int, (List[String], List[String]))]] {
    def zero(m: ArrayBuffer[(Int, (List[String], List[String]))]) =
      new ArrayBuffer[(Int, (List[String], List[String]))]
    def addInPlace(m1: ArrayBuffer[(Int, (List[String], List[String]))]
                   , m2: ArrayBuffer[(Int, (List[String], List[String]))]) = m1 ++ m2
  }


  def apply(records: RDD[ArchiveRecord], sc: SparkContext, k: Int = 20, maxIterations: Int = 2, output: String, limit: Int = 20, numDocs: Int = 20,
            testK: Boolean=false, maxK: Int= 50, minDocThreshold: Int=5, stepK: Int=10, minK: Int=5) = {
    val stopwords = sc.broadcast(Set("a", "as", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "ff", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows", "known", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero"))
    val rec = records.keepValidPages().map(r=>(r.getContentString, r.getUrl)).persist()
    val lemmatized:RDD[Seq[String]] = rec.mapPartitions(r => {
      val stemmer = new EnglishStemmer()
      stemmer.stem()
      r.map(q => getLemmas(q._1, stemmer, stopwords))
    }).persist()
    val lemmaSize = lemmatized.count()

    lemmatized.map(l => (l.length, l)).sortByKey().collect().foreach(pair=>{
      println(pair._1, pair._2)
    })

    val hashingTF = new HashingTF()
    val tfidf = getTfIdf(lemmatized, minDocThreshold, hashingTF).cache()
    val urls = rec.map(r=>r._2).persist()

    if (testK)
      runTestK(tfidf, maxIterations, maxK, sc, stepK, minK)

    val clusters: KMeansModel = KMeans.train(tfidf, k, maxIterations)
    val clusterRdds = getClusterRdds(tfidf, urls, clusters)

    val allWords = lemmatized.flatMap(seq => seq.map(f=>f)).persist()
    val hashIndexToTerm = allWords.map(s=>(hashingTF.indexOf(s), s)).distinct().cache()
    topNWords(output, sc, limit, numDocs, clusterRdds, clusters, hashIndexToTerm)
  }

  def topNWords(output: String, sc: SparkContext, limit: Int = 20, numDocs: Int = 20,
                clusterRdds: ArrayBuffer[(Int, RDD[(Vector, String)])], clusters: KMeansModel, hashIndexToTerm:RDD[(Int, String)]) = {
    val accum = sc.accumulator(new ArrayBuffer[(Int, (List[String], List[String]))])(ArrayAccumulator)
    clusterRdds.par.foreach(c => {
      val cluster = c._2
      val clusterNum = c._1
      val clusterCenter = clusters.clusterCenters(clusterNum)
      val docs = cluster.map(r => (Vectors.sqdist(clusterCenter, r._1), r._2)).takeOrdered(numDocs)(Ordering[Double].on(x => x._1)).map(_._2)

      val topWords = sc.parallelize(clusterCenter.toArray).zipWithIndex.takeOrdered(limit)(Ordering[Double].reverse.on(x=>x._1))
        .map{ case (k, i) => hashIndexToTerm.lookup(i.toInt).mkString(",")}
      accum += ArrayBuffer((clusterNum, (topWords.toList, docs.toList)))//.map(x=>x.mkString(","))))})
    })
    val result = sc.parallelize(accum.value).partitionBy(new HashPartitioner(clusters.k)).map(_._2).saveAsTextFile(output)
  }

  def getClusterRdds(tfidf: RDD[Vector], urls: RDD[String], clusters: KMeansModel) = {
    val rdds = new ArrayBuffer[(Int, RDD[(Vector, String)])]
    val merged = tfidf.zip(urls).map(r=>(r._1, r._2))
    for (i <- 0 to clusters.k-1) {
      rdds += Pair(i, merged.filter(v => clusters.predict(v._1) == i).persist())
    }
    rdds
  }

  def getLemmas(q: String, stemmer: EnglishStemmer, stopwords: Broadcast[Set[String]]): Seq[String] = {
    val text = Jsoup.parse(q).select("body").first().text()
    val lemmas = new ListBuffer[String]()
    val words = text.split("\\s")
    for (word <- words){
      stemmer.setCurrent(word.toLowerCase.replaceAll("[^\\p{L}\\p{Nd}]+", ""))
      stemmer.stem()
      val lemma = stemmer.getCurrent()
      if (lemma.length > 2 && !stopwords.value.contains(lemma)) {
        lemmas += lemma
      }
    }
    lemmas.toList
  }

  def convertToDF(lemmatized: RDD[(String, Seq[String])], sc: SparkContext) = {
    val sqlContext = new SQLContext(sc)
    sqlContext.createDataFrame(lemmatized).toDF("url", "words")
  }

  def getTfIdf(lemmatized: RDD[Seq[String]], minDocThreshold: Int, hashingTF: HashingTF) = {
    val tf = hashingTF.transform(lemmatized)

    tf.cache()
    val idf = new IDF(minDocFreq = minDocThreshold).fit(tf)
    idf.transform(tf)
  }
}
