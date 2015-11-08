package org.warcbase.spark.matchbox

import org.scalatest.FunSuite


class ExtractTopLevelDomainTest extends FunSuite {

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
      case (link, domain) => assert(ExtractTopLevelDomain(link) == domain)
    }
  }
  test("withBase") {
    data2.foreach {
      case (link, base, domain) => assert(ExtractTopLevelDomain(link, base) == domain)
    }
  }
}
