package org.warcbase.spark.scripts

import java.util.Properties
import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SQLContext}


import org.tartarus.snowball.ext.EnglishStemmer
import org.apache.spark.SparkContext
import edu.stanford.nlp.process._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.feature.{IDF, HashingTF}
import org.apache.spark.mllib.linalg
import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup
import org.warcbase.spark.archive.io.ArchiveRecord
import org.warcbase.spark.rdd.RecordRDD._
import scala.collection.JavaConversions._
import scala.collection.mutable.{ListBuffer, ArrayBuffer}

object KMeansRunner {
  def apply(records: RDD[ArchiveRecord], sc: SparkContext) = {
    val stopwords = sc.broadcast(Set("a", "as", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "aint", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another", "any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "arent", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", "cmon", "cs", "came", "can", "cant", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldnt", "course", "currently", "definitely", "described", "despite", "did", "didnt", "different", "do", "does", "doesnt", "doing", "dont", "done", "down", "downwards", "during", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", "far", "few", "ff", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", "had", "hadnt", "happens", "hardly", "has", "hasnt", "have", "havent", "having", "he", "hes", "hello", "help", "hence", "her", "here", "heres", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", "i", "id", "ill", "im", "ive", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isnt", "it", "itd", "itll", "its", "its", "itself", "just", "keep", "keeps", "kept", "know", "knows", "known", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "lets", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", "que", "quite", "qv", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldnt", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", "ts", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "thats", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "theres", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "theyd", "theyll", "theyre", "theyve", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "value", "various", "very", "via", "viz", "vs", "want", "wants", "was", "wasnt", "way", "we", "wed", "well", "were", "weve", "welcome", "well", "went", "were", "werent", "what", "whats", "whatever", "when", "whence", "whenever", "where", "wheres", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whos", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "wont", "wonder", "would", "would", "wouldnt", "yes", "yet", "you", "youd", "youll", "youre", "youve", "your", "yours", "yourself", "yourselves", "zero"))

    val lemmatized = records.keepValidPages().mapPartitions(r => {
      val props = new Properties()
      props.put("annotators", "tokenize, ssplit, pos, lemma")
      val stemmer = new EnglishStemmer()
      stemmer.stem()
      r.map(q => getLemmas(Jsoup.parse(q.getContentString).body().text(), stemmer))
    })

    //val dataSet = convertToDF(lemmatized, sc)
    // new StopWordsRemover().setInputCol("words").transform(dataSet).show()

    val tfidf = getTfIdf(lemmatized).cache()

    // tfidf.saveAsTextFile("/outputtfidf")

    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(tfidf, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(tfidf)
    println("Within Set Sum of Squared Errors = " + WSSSE)
    /*
    val test = lemmatized.map(r => Vectors.dense(r))
    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(lemmatized, numClusters, numIterations)*/

  }

  def getLemmas(text: String, pipeline: StanfordCoreNLP): Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ListBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas.toList
  }

  def getLemmas(text: String, stemmer: EnglishStemmer, stopwords: Broadcast[Set[String]]): Seq[String] = {
    val lemmas = new ListBuffer[String]()
    val words = text.split(" ")
    for (word <- words){
      stemmer.setCurrent(word.toLowerCase.replaceAll("[^\\p{L}\\p{Nd}]+", ""))
      stemmer.stem()
      val lemma = stemmer.getCurrent()
      println(lemma)
      if (lemma.length > 2 && !stopwords.value.contains(lemma)) {
        lemmas += lemma
      }
    }
    lemmas.toList
  }

  def getLemmas(text: String, stemmer: EnglishStemmer): Seq[String] = {
    val lemmas = new ListBuffer[String]()
    val words = text.split(" ")
    for (word <- words){
      stemmer.setCurrent(word.toLowerCase.replaceAll("[^\\p{L}\\p{Nd}]+", ""))
      stemmer.stem()
      val lemma = stemmer.getCurrent()
      println(lemma)
      if (lemma.length > 2) {
        lemmas += lemma
      }
    }
    lemmas.toList
  }

  def convertToDF(lemmatized: RDD[(String, Seq[String])], sc: SparkContext) = {
    val sqlContext = new SQLContext(sc)
    sqlContext.createDataFrame(lemmatized).toDF("url", "words")
  }

  def getTfIdf(lemmatized: RDD[Seq[String]]) = {
    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(lemmatized)

    tf.cache()
    val idf = new IDF().fit(tf)
    idf.transform(tf)
  }
}
