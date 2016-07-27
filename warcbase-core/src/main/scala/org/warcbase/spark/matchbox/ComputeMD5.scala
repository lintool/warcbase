package org.warcbase.spark.matchbox

import java.security.MessageDigest


/**
  * compute MD5 checksum..
  *
  */
object ComputeMD5 {
  /**
    *
    * @param bytes
    * @return
    */
  def apply(bytes: Array[Byte]): String = {
    new String(MessageDigest.getInstance("MD5").digest(bytes))
  }
}