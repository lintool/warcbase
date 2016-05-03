package org.warcbase.spark.matchbox

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.warcbase.spark.utils.JsonUtil
import scala.collection.mutable.MutableList
import scala.util.Random

/**
  * Classifies records using NER and stores results as JSON
  */

class NERCombinedJson extends Serializable {
  case class StringList(list: List[String]) // Used to prevent unchecked type-argument error

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
    val line = inFile.readLine()
    if (line != null) outFile.write(line)    
    Iterator.continually(inFile.readLine()).takeWhile(_ != null).foreach(s => {outFile.write(", " + s)})
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
            val substrs = line.split(",", 3)
            (substrs(0), substrs(1), substrs(2))
          })
          .map(r => {
            val classifiedJson = NER3Classifier.classify(r._3)
            val classifiedMap = JsonUtil.fromJson(classifiedJson)
            val classifiedMapCountTuples: Map[String, List[(String, Int)]] = classifiedMap.map {
              case (nerType, entities: StringList) => (nerType, entities.list.groupBy(identity).mapValues(_.size).toList)
            }
            ((r._1, r._2), classifiedMapCountTuples)
          })
      })
      .reduceByKey( (a, b) => (a ++ b).keySet.map(r => (r, combineKeyCountLists(a(r), b(r)))).toMap)
      .mapPartitions(iter => {
        iter.map(r => {
          val nerRec = new NerRecord(r._1._1, r._1._2)
          r._2.foreach(entityMap => {  
            // e.g., entityMap = "PERSON" -> List(("Jack", 1), ("Diane", 3))
            val ec = new EntityCounts(entityMap._1)    
            entityMap._2.foreach(e => {
              ec.entities += new Entity(e._1, e._2)
            })
            nerRec.ner += ec
          })
          JsonUtil.toJson(nerRec)
        })
      })
      .saveAsTextFile(outputFile)

    partDirToFile(outputFile)
  }

  class Entity(iEntity: String, iFreq: Int) {
    var entity: String = iEntity
    var freq: Int = iFreq
  }

  class EntityCounts(iNerType: String) { 
    var nerType: String = iNerType
    var entities = MutableList[Entity]()
  }

  class NerRecord(recDate: String, recDomain: String) {
    var date = recDate
    var domain = recDomain

    var ner = MutableList[EntityCounts]()
  }
}


