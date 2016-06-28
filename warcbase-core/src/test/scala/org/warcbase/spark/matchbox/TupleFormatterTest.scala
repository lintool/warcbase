package org.warcbase.spark.matchbox

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TupleFormatterTest extends FunSuite {
  test("tab delimit") {
    val tuple = (("a", "b", ("c", 9)), "d", 5, ("hi", 1))
    assert(TupleFormatter.tabDelimit(tuple) == "a\tb\tc\t9\td\t5\thi\t1")
  }
}
