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

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractUrlsTest extends FunSuite {
  test("simple") {
    val tweet = "Tweet with http://t.co/sgeexaad and http://twitter.com/ and https://twitter.com/lintool/"
    val extracted = ExtractUrls(tweet).toList
    assert(extracted.size == 3)
    assert("http://t.co/sgeexaad" == extracted(0))
    assert("http://twitter.com/" == extracted(1))
    assert("https://twitter.com/lintool/" == extracted(2))
  }
}
