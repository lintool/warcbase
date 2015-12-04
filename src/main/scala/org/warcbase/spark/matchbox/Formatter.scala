package org.warcbase.spark.matchbox

object Formatter {
  def tabDelimit(it: Iterator[Any]): String = {
    it.map(i => i match {
      case s: String => s
      case d: Int => d
      case i: Iterator[Any] => tabDelimit(i)
      case t2: (Any, Any) => tabDelimit(t2.productIterator)
      case t3: (Any, Any, Any) => tabDelimit(t3.productIterator)
      case t4: (Any, Any, Any, Any) => tabDelimit(t4.productIterator)
    }).mkString("\t")
  }
}
