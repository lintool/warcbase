package org.warcbase.spark.matchbox

import shapeless._  // v2.0.0, for full compatibility with Scala 2.10.4 (Spark dep)
import ops.tuple.FlatMapper
import ops.tuple.ToList
import syntax.std.tuple._

object TupleFormatter {
  /** Borrowed from shapeless's flatten.scala example */
  trait LowPriorityFlatten extends Poly1 {
    implicit def default[T] = at[T](Tuple1(_))
  }

  /**
    * Flattens nested tuples
    *
    * Takes as argument a tuple of any size
    */
  object flatten extends LowPriorityFlatten {
    implicit def caseTuple[T <: Product](implicit fm: FlatMapper[T, flatten.type]) =
      at[T](_.flatMap(flatten))
  }

  /**
    * Transforms a tuple into a tab-delimited string, flattening any nesting
    *
    * Takes as argument a tuple of any size
    */
  object tabDelimit extends Poly1 {
    implicit def caseTuple[T <: Product, Lub](implicit tl: ToList[T, Lub], fm: FlatMapper[T, flatten.type]) =
      at[T](flatten(_).asInstanceOf[Product].productIterator.mkString("\t"))
  }
}