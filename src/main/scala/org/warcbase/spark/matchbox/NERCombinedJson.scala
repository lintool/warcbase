package org.warcbase.spark.matchbox

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.InputStreamReader
import java.io.OutputStreamWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.rdd.RDD
import org.warcbase.spark.utils.JsonUtil
import scala.util.Random

/**
  * Classifies records using NER and stores results as JSON
  */

object NERCombinedJson extends Serializable {
  class EntityCounts(iNerType: String, iEntities: List[(String, Int)]) {
    var nerType = iNerType
    var entities = iEntities
  }

  class NerRecord(recDate: String, recDomain: String, entityMap: List[EntityCounts]) {
    var date = recDate
    var domain = recDomain
    var ner = entityMap
  }

  /** Do NER classification on input RDD of tuples, output JSON.
    *
    * @param iNerClassifierFile path of classifier file
    * @param rdd RDD of tuples (date: String, url: String, content: String)
    *            from which to extract entities
    */
  def classify(iNerClassifierFile: String, rdd: RDD[(String, String, String)]): RDD[String] = {
    rdd.mapPartitions(iter => {
      NER3Classifier.apply(iNerClassifierFile)
      iter.map(r => {
        val classifiedJson = NER3Classifier.classify(r._3)  // TODO: Should classify to Map
        val classifiedMap = JsonUtil.fromJson(classifiedJson)
        val classifiedMapCountTuples = classifiedMap.map {
          case (nerType, entityList: List[String]) => (nerType, entityList.groupBy(identity).map(r => (r._1, r._2.size)).toList)
        }
        ((r._1, r._2), classifiedMapCountTuples)
      })
    })
    .reduceByKey((a, b) => (a ++ b).keySet.map(k => (k, combineCountLists(a(k), b(k)))).toMap)
    .map(r => (r._1, r._2.map(x => new EntityCounts(x._1, x._2)).toList))
    .map(r => JsonUtil.toJson(new NerRecord(r._1._1, r._1._2, r._2)))
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

  def combineCountLists(l1: List[(String, Int)], l2: List[(String, Int)]): List[(String, Int)] = {
    (l1 ++ l2).groupBy(_._1).map{ case (k,v) => (k, v.map(_._2).sum) }.toList
  }
}


