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
class ExtractAtMentionsTest extends FunSuite {
  test("simple") {
    val tweet = ":@lintool What's @tedcruz's beef @realDonaldTrump?"
    val extracted = ExtractAtMentions(tweet).toList
    assert(extracted.size == 3)
    assert("@lintool" == extracted(0))
    assert("@tedcruz" == extracted(1))
    assert("@realDonaldTrump" == extracted(2))
  }
}
