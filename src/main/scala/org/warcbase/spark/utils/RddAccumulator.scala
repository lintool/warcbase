package org.warcbase.spark.utils

import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD

/**
  * Created by youngbinkim on 5/22/16.
  */
class RddAccumulator[T] extends AccumulatorParam[RDD[T]] {
  def zero(initialValue: RDD[T]): RDD[T] = {
    initialValue
  }
  def addInPlace(v1: RDD[T], v2: RDD[T]): RDD[T]= {
    v1.union(v2)
  }
}
