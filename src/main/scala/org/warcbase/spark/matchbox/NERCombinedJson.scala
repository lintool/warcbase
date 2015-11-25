package org.warcbase.spark.matchbox

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import scala.util.Random

/**
  * Classifies records using NER and stores results as JSON
  */

class NERCombinedJson extends Serializable {
  def combineKeyCountLists (l1: List[(String, Int)], l2: List[(String, Int)]): List[(String, Int)] = {
    (l1 ++ l2).groupBy(_._1 ).map {
      case (key, tuples) => (key, tuples.map( _._2).sum) 
    }.toList
  }

  /** Combines directory of part-files containing one JSON array per line
    * into a single file containing a single JSON array of arrays.
    *
    * @param srcDir name of directory holding files, also name that will
    *               be given to JSON file.
    */
  def partDirToFile(srcDir: String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val rnd = new Random
    
    val srcPath = new Path(srcDir)
    val tmpFile = rnd.alphanumeric.take(8).mkString + ".almostjson"
    val tmpPath = new Path(tmpFile)

    // Merge part-files into single file
    FileUtil.copyMerge(hdfs, srcPath, hdfs, tmpPath, false, hadoopConfig, null)

    // Read file of JSON arrays, write into single JSON array of arrays
    val fsInStream = hdfs.open(tmpPath)
    val inFile = new BufferedReader(new InputStreamReader(fsInStream))
    hdfs.delete(srcPath, true)  // Don't need part-files anymore
    val fsOutStream = hdfs.create(srcPath, true) // path was dir of part-files,
                                                 // now is a file of JSON
    val outFile = new BufferedWriter(new OutputStreamWriter(fsOutStream))
    outFile.write("[")
    Iterator.continually(inFile.readLine()).takeWhile(_ != null).foreach(outFile.write)
    outFile.write("]")
    outFile.close()
    
    inFile.close()
    hdfs.delete(tmpPath, false)
  }

  /** Do NER classification on input path, output JSON.
    *
    * @param iNerClassifierFile path of classifier file
    * @param inputFile path of file with tuples (date: String, url: String, content: String)
    *                  from which to extract entities
    * @param outputFile path of output file (e.g., "entities.json")
    * @param sc Spark context object
    */
  def classify(iNerClassifierFile: String, inputFile: String, outputFile: String, sc: SparkContext) {
    val out = sc.textFile(inputFile)
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

    partDirToFile(outputFile)
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
