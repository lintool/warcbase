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
  * UDF for extracting image links from a webpage given the HTML content (using Jsoup).
  */
object ExtractImageLinks {
  /**
    * @param src the src link
    * @param html the content from which links are to be extracted
    *
    * Returns a sequence of image links
    */
  def apply(src: String, html: String): Seq[String] = {
    if (html.isEmpty) return Nil
    try {
      val output = mutable.MutableList[String]()
      val doc = Jsoup.parse(html)
      val links: Elements = doc.select("img[src]")
      val it = links.iterator()
      while (it.hasNext) {
        val link = it.next()
        link.setBaseUri(src)
        val target = link.attr("abs:src")
        output += (target)
      }
      output
    } catch {
      case e: Exception =>
        throw new IOException("Caught exception processing input ", e);
    }
  }
}
