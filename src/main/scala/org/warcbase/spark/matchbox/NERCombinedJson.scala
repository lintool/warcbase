package org.warcbase.spark.matchbox

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

/**
  * Classifies records using NER and stores results as JSON
  */

class NERCombinedJson extends Serializable {
  def combineKeyCountLists (l1: List[(String, Int)], l2: List[(String, Int)]): List[(String, Int)] = {
    (l1 ++ l2).groupBy(_._1 ).map {
      case (key, tuples) => (key, tuples.map( _._2).sum) 
    }.toList
  }

  def classify(iNerClassifierFile: String, inputRecordFile: String, outputFile: String, sc: SparkContext) {
    val out = sc.textFile(inputRecordFile)
      .mapPartitions(iter => {
        NER3Classifier.apply(iNerClassifierFile)
        iter.map(line => {
            val ind1 = line.indexOf(",")
            val ind2 = line.indexOf(",", ind1 + 1)
            (line.substring(1, ind1),
            line.substring(ind1 + 1, ind2),
            line.substring(ind2 + 1, line.length - 1))
          })
          .map(r => {
            val classifiedJson = NER3Classifier.classify(r._3)
            val jUtl = new JsonUtil
            val classifiedMap = jUtl.fromJson[Map[String,List[String]]](classifiedJson)
            val classifiedMapCountTuples: Map[String, List[(String, Int)]] = classifiedMap.map {
              case (nerType, entityList) => (nerType, entityList.groupBy(identity).mapValues(_.size).toList)
            }
            ((r._1, r._2), classifiedMapCountTuples)
          })
      })
      .reduceByKey( (a, b) => (a ++ b).keySet.map(r => (r, combineKeyCountLists(a(r), b(r)))).toMap)
      .mapPartitions(iter => {
        val jUtl = new JsonUtil
        iter.map(r => {
          (jUtl.toJson(r))
        })
      })
      .saveAsTextFile(outputFile)  
  }
}

class JsonUtil extends Serializable {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def toJson(value: Map[Symbol, Any]): String = {
    toJson(value map { case (k,v) => k.name -> v})
  }

  def toJson(value: Any): String = {
    mapper.writeValueAsString(value)
  }

  def fromJson[T](json: String)(implicit m : Manifest[T]): T = {
    mapper.readValue[T](json)
  }
}
