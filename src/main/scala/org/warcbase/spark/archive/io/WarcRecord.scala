package org.warcbase.spark.archive.io

import java.text.SimpleDateFormat

import org.apache.spark.SerializableWritable
import org.archive.util.ArchiveUtils
import org.warcbase.data.WarcRecordUtils
import org.warcbase.io.WarcRecordWritable
import org.warcbase.spark.matchbox.ExtractTopLevelDomain

implicit class WarcRecord(r: SerializableWritable[WarcRecordWritable]) extends ArchiveRecord {
  val ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

  val getCrawldate: String = ArchiveUtils.get14DigitDate(ISO8601.parse(r.t.getRecord.getHeader.getDate)).substring(0, 8)

  val getContentBytes: Array[Byte] = WarcRecordUtils.getContent(r.t.getRecord)

  val getContentString: String = new String(getContentBytes)

  val getMimeType = WarcRecordUtils.getWarcResponseMimeType(getContentBytes)

  val getUrl = r.t.getRecord.getHeader.getUrl

  val getDomain = ExtractTopLevelDomain(getUrl)
}
