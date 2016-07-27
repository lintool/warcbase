package org.warcbase.spark.archive.io

import java.text.SimpleDateFormat

import org.apache.spark.SerializableWritable
import org.archive.util.ArchiveUtils
import org.warcbase.data.WarcRecordUtils
import org.warcbase.io.WarcRecordWritable
import org.warcbase.spark.matchbox.ExtractDate.DateComponent
import org.warcbase.spark.matchbox.{RemoveHttpHeader, ExtractDate, ExtractDomain}

class WarcRecord(r: SerializableWritable[WarcRecordWritable]) extends ArchiveRecord {
  val ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

  val getCrawlDate: String = ExtractDate(ArchiveUtils.get14DigitDate(ISO8601.parse(r.t.getRecord.getHeader.getDate)), DateComponent.YYYYMMDD)

  val getCrawlMonth: String = ExtractDate(ArchiveUtils.get14DigitDate(ISO8601.parse(r.t.getRecord.getHeader.getDate)), DateComponent.YYYYMM)

  val getContentBytes: Array[Byte] = WarcRecordUtils.getContent(r.t.getRecord)

  val getContentString: String = new String(getContentBytes)

  val getMimeType = WarcRecordUtils.getWarcResponseMimeType(getContentBytes)

  val getUrl = r.t.getRecord.getHeader.getUrl

  val getDomain = ExtractDomain(getUrl)

  val getImageBytes: Array[Byte] = {
    if (getContentString.startsWith("HTTP/"))
      getContentBytes.slice(
        getContentString.indexOf(RemoveHttpHeader.headerEnd)
          + RemoveHttpHeader.headerEnd.length, getContentBytes.length)
    else
      getContentBytes
  }
}
