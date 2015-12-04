package org.warcbase.spark.matchbox

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractDateTest extends FunSuite {

  test("simple") {
    assert(ExtractDate("20151204", ExtractDate.Date.YYYY) == "2015")
    assert(ExtractDate("20151204", ExtractDate.Date.MM) == "12")
    assert(ExtractDate("20151204", ExtractDate.Date.DD) == "04")
    assert(ExtractDate("20151204", ExtractDate.Date.YYYYMM) == "201512")
    assert(ExtractDate("20151204", ExtractDate.Date.YYYYMMDD) == "20151204")
    assert(ExtractDate(null, ExtractDate.Date.YYYYMMDD) == null)
  }
}
