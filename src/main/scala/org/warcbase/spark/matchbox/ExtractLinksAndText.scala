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

import java.io.IOException

import org.jsoup.Jsoup
import org.jsoup.select.Elements

import scala.collection.mutable

/**
 * UDF for extracting links from a webpage given the HTML content (using Jsoup). Returns a seq of tuples,
 * where each tuple consists of the URL and the anchor text.
 */
object ExtractLinksAndText {
  def apply(html: String, base: String): Seq[(String, String)] = {

    if (html.isEmpty) return Nil

    try {
      val output = mutable.MutableList[(String, String)]()

      val doc = Jsoup.parse(html)
      val links: Elements = doc.select("a[href]")

      val it = links.iterator()
      while (it.hasNext) {
        val link = it.next()
        if (base.nonEmpty) link.setBaseUri(base)
        val target = link.attr("abs:href")
        if (target.nonEmpty) {
          output += ((target, link.text))
        }
      }
      output
    } catch {
      case e: Exception =>
        throw new IOException("Caught exception processing input row ", e);
    }
  }
}