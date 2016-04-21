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

package org.warcbase.spark.scripts

import org.apache.spark.SparkContext
import org.warcbase.spark.matchbox.{ExtractLinks, RecordLoader}
import org.warcbase.spark.rdd.RecordRDD._

object SocialMediaLinks {

  def socialMediaLinksCount(sc: SparkContext) = {
    RecordLoader.loadArc("/shared/collections/CanadianPoliticalParties/arc/", sc)
      .map(r => (r.getCrawlDate, r.getDomain, ExtractLinks(r.getUrl, r.getContentString)))
      .flatMap(r => r._3
        .filter(f => f._2.matches(".*(twitter|facebook|youtube).*"))
        .map(f => (r._1, r._2, f._2)))
      .countItems()
      .saveAsTextFile("cpp.socialmedia/")
  }
}
