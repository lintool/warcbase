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
class ExtractImageLinksTest extends FunSuite {
  test("simple") {
    val fragment =
      """Image here: <img src="http://foo.bar.com/pic.png"> and another <img src="http://baz.org/a/b/banner.jpg"/>"""
    val extracted = ExtractImageLinks("", fragment).toList
    assert(extracted.size == 2)
    assert("http://foo.bar.com/pic.png" == extracted(0))
    assert("http://baz.org/a/b/banner.jpg" == extracted(1))
  }

  test("relative") {
    val fragment =
      """Image here: <img src="pic.png"> and another <img src="http://baz.org/a/b/banner.jpg"/> and <img src="../logo.gif"/>"""
    val extracted = ExtractImageLinks("http://foo.bar.com/a/page.html", fragment)
    assert(extracted.size == 3)
    assert("http://foo.bar.com/a/pic.png" == extracted(0))
    assert("http://baz.org/a/b/banner.jpg" == extracted(1))
    assert("http://foo.bar.com/logo.gif" == extracted(2))
  }
}
