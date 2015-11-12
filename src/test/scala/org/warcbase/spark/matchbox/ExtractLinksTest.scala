package org.warcbase.spark.matchbox

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ExtractLinksTest extends FunSuite {
  test("simple") {
    val fragment: String = "Here is <a href=\"http://www.google.com\">a search engine</a>.\n" + "Here is <a href=\"http://www.twitter.com/\">Twitter</a>.\n"
    val extracted: Seq[(String, String, String)] = ExtractLinks("", fragment)
    assert(extracted.size == 2)
    assert("http://www.google.com" == extracted.head._2)
    assert("a search engine" == extracted.head._3)
    assert("http://www.twitter.com/" == extracted.last._2)
    assert("Twitter" == extracted.last._3)
  }

  test("relative") {
    val fragment: String = "Here is <a href=\"http://www.google.com\">a search engine</a>.\n" + "Here is <a href=\"page.html\">a relative URL</a>.\n"
    val extracted: Seq[(String, String, String)] = ExtractLinks("", fragment, "http://www.foobar.org/index.html")
    assert(extracted.size == 2)
    assert("http://www.google.com" == extracted.head._2)
    assert("a search engine" == extracted.head._3)
    assert("http://www.foobar.org/page.html" == extracted.last._2)
    assert("a relative URL" == extracted.last._3)
  }
}
