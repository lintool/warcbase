package org.warcbase.spark.matchbox

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import org.warcbase.spark.matchbox.ExtractDate.DateComponent._

@RunWith(classOf[JUnitRunner])
class ExtractDateTest extends FunSuite {

  test("simple") {
    assert(ExtractDate("20151204", YYYY) == "2015")
    assert(ExtractDate("20151204", MM) == "12")
    assert(ExtractDate("20151204", DD) == "04")
    assert(ExtractDate("20151204", YYYYMM) == "201512")
    assert(ExtractDate("20151204", YYYYMMDD) == "20151204")
    assert(ExtractDate(null, YYYYMMDD) == null)
  }
}
