package org.warcbase.spark.matchbox

import java.util

import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.ling.CoreLabel
import scala.collection.mutable

/**
  * UDF which reads in a text string, and returns entities identified by the configured Stanford NER classifier
  */
object NER3Classifier {
  var serializedClassifier: String = _
  var classifier: AbstractSequenceClassifier[CoreLabel] = _

  object NERClassType extends Enumeration {
    type NERClassType = Value
    val PERSON, ORGANIZATION, LOCATION, O = Value
  }

  def apply(file: String) = {
    serializedClassifier = file
  }

  def classify(input: String): String = {
    val emptyString: String = "{PERSON=[], ORGANIZATION=[], LOCATION=[]}"
    val entitiesByType = scala.collection.mutable.Map[NERClassType.Value, mutable.Seq[String]]()
    for (t <- NERClassType.values) {
      if (t != NERClassType.O) entitiesByType.put(t, mutable.Seq())
    }

    var prevEntityType = NERClassType.O
    var entityBuffer: String = ""

    if (input == null) return emptyString

    try {
      if (input == null) return emptyString
      if (classifier == null) classifier = CRFClassifier.getClassifier(serializedClassifier)
      val out: util.List[util.List[CoreLabel]] = classifier.classify(input)
      val outit = out.iterator()
      while (outit.hasNext) {
        val sentence = outit.next
        val sentenceit = sentence.iterator()
        while (sentenceit.hasNext) {
          val word = sentenceit.next
          val wordText = word.word()
          val classText = word.get(classOf[CoreAnnotations.AnswerAnnotation])
          val currEntityType = NERClassType.withName(classText)
          if (prevEntityType != currEntityType) {
            if (prevEntityType != NERClassType.O && !entityBuffer.equals("")) {
              //time to commit
              entitiesByType.put(prevEntityType, entitiesByType.get(prevEntityType).get ++ Seq(entityBuffer))
              entityBuffer = ""
            }
          }
          prevEntityType = currEntityType
          if (currEntityType != NERClassType.O) {
            if (entityBuffer.equals(""))
              entityBuffer = wordText
            else
              entityBuffer += " " + wordText
          }
        }
        //end of sentence
        //apply commit and reset
        if (prevEntityType != NERClassType.O && !entityBuffer.equals("")) {
          entitiesByType.put(prevEntityType, entitiesByType.get(prevEntityType).get ++ Seq(entityBuffer))
          entityBuffer = ""
        }
        //reset
        prevEntityType = NERClassType.O
        entityBuffer = ""
      }

      entitiesByType.toString()
    }
    catch {
      case e: Exception => {
        if (classifier == null) throw new ExceptionInInitializerError("Unable to load classifier " + e)
        emptyString
      }
    }
  }

}
