package org.warcbase.spark.scripts

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, TokensAnnotation, SentencesAnnotation}
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import org.apache.spark.rdd.RDD
import org.jsoup.Jsoup
import org.warcbase.spark.archive.io.ArchiveRecord
import org.warcbase.spark.rdd.RecordRDD._
import edu.stanford.nlp.pipeline.Annotation
import java.util.Properties

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

object KMeansRunner {
  def apply(records: RDD[ArchiveRecord]) = {
    val lemmatized = records.keepValidPages().mapPartitions(r => {
      val props = new Properties()
      props.put("annotators", "tokenize, ssplit, pos, lemma")
      val pipeline = new StanfordCoreNLP(props)

      r.map(q => getLemmas(Jsoup.parse(q.getContentString).body().text(), pipeline))
    })
    lemmatized.foreach(println)
  }

  def getLemmas(text: String, pipeline: StanfordCoreNLP): Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas
  }
}
