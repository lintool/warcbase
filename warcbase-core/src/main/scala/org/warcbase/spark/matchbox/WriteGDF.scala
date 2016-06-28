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

import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths
import org.apache.spark.rdd.RDD

/** 
  * UDF for exporting an RDD representing a collection of links to a GDF file.
  */

object WriteGDF {
  /**
  * @param rdd RDD of elements in format ((datestring, source, target), count).
  * @param gdfPath Output file.
  *
  * Writes graph nodes and edges to file.
  */
  def apply(rdd: RDD[((String, String, String), Int)], gdfPath: String): Unit = {
    if (gdfPath == "") return

    val outFile = Files.newBufferedWriter(Paths.get(gdfPath), StandardCharsets.UTF_8)

    val edges = rdd.map(r => (r._1._2, r._1._3, r._2, r._1._1)).collect
    val nodes = rdd.flatMap(r => List(r._1._2, r._1._3)).distinct.collect

    outFile.write("nodedef> name VARCHAR\n")
    nodes.foreach(r => outFile.write(r + "\n"))
    outFile.write("edgedef> source VARCHAR, target VARCHAR, weight DOUBLE, timeint VARCHAR\n")
    edges.foreach(r => outFile.write(r.productIterator.toList.mkString(",") + "\n"))
    outFile.close()
  }
}
