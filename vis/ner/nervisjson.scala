import org.apache.spark.rdd.RDD
import org.warcbase.spark.matchbox.NER3Classifier
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

class JsonUtil extends Serializable {
  val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def fromJson[T](json: String)(implicit m : Manifest[T]): T = {
    mapper.readValue[T](json)
  }
}

def combineKeyCountLists (l1: List[(String, Int)], l2: List[(String, Int)]): List[(String, Int)] = {
  (l1 ++ l2).groupBy(_._1 ).map {
    case (key, tuples) => (key, tuples.map( _._2).sum) 
  }.toList
}

sc.addFile("/cliphomes/jrwiebe/english.all.3class.distsim.crf.ser.gz")

val out = sc.textFile("hdfs:///user/jrwiebe/cpp.greenparty-arc-plaintext/")
  .mapPartitions(iter => {
    NER3Classifier.apply("english.all.3class.distsim.crf.ser.gz")
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
  .saveAsTextFile("nervis/")

