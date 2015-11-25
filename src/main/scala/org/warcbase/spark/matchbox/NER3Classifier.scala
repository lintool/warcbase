/*
 * Warcbase: an open-source platform for managing web archives
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.warcbase.spark.matchbox

import java.util

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, JsonScalaEnumeration}
import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.{CoreAnnotations, CoreLabel}

import scala.collection.mutable

/**
  * UDF which reads in a text string, and returns entities identified by the configured Stanford NER classifier
  */
object NER3Classifier {

  var serializedClassifier: String = _
  var classifier: AbstractSequenceClassifier[CoreLabel] = _
  val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  object NERClassType extends Enumeration {
    type NERClassType = Value
    val PERSON, ORGANIZATION, LOCATION, O = Value

    class NERClassTypeType extends TypeReference[NERClassType.type]

    case class NERClassTypeHolder(@JsonScalaEnumeration(classOf[NERClassTypeType]) nerclasstype: NERClassType.NERClassType)

  }

  def apply(file: String) = {
    serializedClassifier = file
  }

  def classify(input: String): String = {
    val emptyString: String = "{\"PERSON\":[],\"ORGANIZATION\"=[],\"LOCATION\"=[]}"
    val entitiesByType = mutable.LinkedHashMap[NERClassType.Value, mutable.Seq[String]]()
    for (t <- NERClassType.values) {
      if (t != NERClassType.O) entitiesByType.put(t, mutable.Seq())
    }

    var prevEntityType = NERClassType.O
    var entityBuffer: String = ""

    if (input == null) return emptyString

    try {
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

      mapper.writeValueAsString(entitiesByType)
    } catch {
      case e: Exception =>
        if (classifier == null) throw new ExceptionInInitializerError("Unable to load classifier " + e)
        emptyString
    }
  }
}
