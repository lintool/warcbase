package org.warcbase.spark.archive.io

trait ArchiveRecord extends Serializable {
  val getCrawldate: String

  val getCrawlmonth: String

  val getUrl: String

  val getDomain: String

  val getMimeType: String

  val getContentString: String

  val getContentBytes: Array[Byte]
}
