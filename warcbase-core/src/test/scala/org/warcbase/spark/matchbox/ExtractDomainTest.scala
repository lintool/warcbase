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
class ExtractDomainTest extends FunSuite {

  private val data1: Seq[(String, String)] = Seq.newBuilder.+=(
    ("http://www.umiacs.umd.edu/~jimmylin/", "www.umiacs.umd.edu"),
    ("https://github.com/lintool", "github.com"),
    ("http://ianmilligan.ca/2015/05/04/iipc-2015-slides-for-warcs-wats-and-wgets-presentation/", "ianmilligan.ca"),
    ("index.html", null)).result()

  private val data2 = Seq.newBuilder.+=(
    ("index.html", "http://www.umiacs.umd.edu/~jimmylin/", "www.umiacs.umd.edu"),
    ("index.html", "lintool/", null)).result()

  test("simple") {
    data1.foreach {
      case (link, domain) => assert(ExtractDomain(link) == domain)
    }
  }
  test("withBase") {
    data2.foreach {
      case (link, base, domain) => assert(ExtractDomain(link, base) == domain)
    }
  }
}
