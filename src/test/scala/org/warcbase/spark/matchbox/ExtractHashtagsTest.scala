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
class ExtractHashtagTest extends FunSuite {
  test("simple") {
    val tweet = "Here are #some #hashtags #TeSTing"
    val extracted = ExtractHashtags(tweet).toList
    assert(extracted.size == 3)
    assert("#some" == extracted(0))
    assert("#hashtags" == extracted(1))
    assert("#TeSTing" == extracted(2))
  }
}
