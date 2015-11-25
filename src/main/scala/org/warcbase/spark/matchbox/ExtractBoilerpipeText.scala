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
import de.l3s.boilerpipe.extractors.DefaultExtractor

/**
 * UDF for extracting raw text content from an HTML page, minus "boilerplate"
 * content (using boilerpipe).
 */
object ExtractBoilerpipeText {
  def apply(input: String) = {
    if (input.isEmpty) Nil
    else
      try {
        val text = DefaultExtractor.INSTANCE.getText(input).replaceAll("[\\r\\n]+", " ").trim()
        if (text.isEmpty) Nil
        else text
      } catch {
        case e: Exception =>
          throw new IOException("Caught exception processing input row " + e)
      }
  }
}


