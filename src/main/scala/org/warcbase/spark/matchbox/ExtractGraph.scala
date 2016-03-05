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

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.warcbase.spark.archive.io.ArchiveRecord
import org.warcbase.spark.rdd.RecordRDD._
import org.warcbase.spark.utils.JsonUtil

/**
  *
  * e.g. when done:
  * $ cat nodes.partjson/part-* > nodes.json && cat links.partjson/part-* > links.json
  * $ jq -c -n --slurpfile nodes nodes.json --slurpfile links links.json '{nodes: $nodes, links: $links}' > graph.json
  *
  */

object ExtractGraph {
  def pageHash(url: String): VertexId = {
    url.hashCode.toLong
  }

  case class RichVertex(domain: String, pageRank: Double, inDegree: Int, outDegree: Int)
  case class RichEdge(date: String, src: String, dst: String, count: Long)

  def apply(records: RDD[ArchiveRecord], nodesPath: String, linksPath: String) {
    val vertices: RDD[(VertexId, RichVertex)] = records.keepValidPages()
      .flatMap(r => ExtractLinks(r.getUrl, r.getContentString))
      .flatMap(r => List(ExtractTopLevelDomain(r._1).replaceAll("^\\s*www\\.", ""), ExtractTopLevelDomain(r._2).replaceAll("^\\s*www\\.", "")))
      .distinct
      .map(r => (pageHash(r), RichVertex(r, 0.0, 0, 0)))

    val edges: RDD[Edge[RichEdge]] = records.keepValidPages()
      .map(r => (r.getCrawldate, ExtractLinks(r.getUrl, r.getContentString)))
      .flatMap(r => r._2.map(f => (r._1, ExtractTopLevelDomain(f._1).replaceAll("^\\s*www\\.", ""), ExtractTopLevelDomain(f._2).replaceAll("^\\s*www\\.", ""))))
      .filter(r => r._2 != null && r._3 != null)
      .countItems()
      .map(r => Edge(pageHash(r._1._2), pageHash(r._1._3), RichEdge(r._1._1, r._1._2, r._1._3, r._2)))

    val graph = Graph(vertices, edges)

    val richGraph = graph.outerJoinVertices(graph.inDegrees) {
      case (vid, rv, inDegOpt) => RichVertex(rv.domain, rv.pageRank, inDegOpt.getOrElse(0), rv.outDegree)
    }.outerJoinVertices(graph.outDegrees) {
      case (vid, rv, outDegOpt) => RichVertex(rv.domain, rv.pageRank, rv.inDegree, outDegOpt.getOrElse(0))
    }.outerJoinVertices(graph.pageRank(0.00001).vertices) {
      case (vid, rv, pageRankOpt) => RichVertex(rv.domain, pageRankOpt.getOrElse(0.0), rv.inDegree, rv.outDegree)
    }

    richGraph.vertices.map(r => JsonUtil.toJson(r._2)).saveAsTextFile(nodesPath)
    richGraph.edges.map(r => JsonUtil.toJson(r.attr)).saveAsTextFile(linksPath)
  }
}

