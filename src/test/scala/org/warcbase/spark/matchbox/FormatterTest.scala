package org.warcbase.spark.matchbox

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FormatterTest extends FunSuite {
  test("tab delimit") {
    val tuple = (("a", "b", "c"), "d", 5, ("hi", 1))
    assert(Formatter.tabDelimit(tuple.productIterator) == "a\tb\tc\td\t5\thi\t1")
  }
}
