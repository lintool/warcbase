package org.warcbase.spark.matchbox

object TupleFormatter {
  /**
    * Flattens and formats nested tuples, delimited with tabs
    *
    * @param it Iterator of tuples
    * @return Flattened tuples as a string
    */
  def tabDelimit(it: Iterator[Any]): String = {
    flatten(it).mkString("\t")
  }

  /**
    * Flattens iterator of nested tuples
    * @param it Iterator of tuples
    * @return Flattened iterator
    */
  def flatten(it: Iterator[Any]): Iterator[Any] = {
    it.map(i => i match {
      case s: String => s
      case d: Int => d
      case t2: (Any, Any) => tabDelimit(t2.productIterator)
      case t3: (Any, Any, Any) => tabDelimit(t3.productIterator)
      case t4: (Any, Any, Any, Any) => tabDelimit(t4.productIterator)
    })
  }
}
