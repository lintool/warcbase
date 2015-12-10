package org.warcbase.spark.archive.io

import java.text.SimpleDateFormat

import org.archive.io.warc.WARCRecord
import org.archive.util.ArchiveUtils
import org.warcbase.data.WarcRecordUtils
import org.warcbase.spark.matchbox.ExtractTopLevelDomain

class WarcRecord(r: WARCRecord) extends ArchiveRecord {
  val ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

  lazy val getCrawldate: String = ArchiveUtils.get14DigitDate(ISO8601.parse(r.getHeader.getDate)).substring(0, 8)

  lazy val getDomain = ExtractTopLevelDomain(getUrl).replace("^\\s*www\\.", "")

  lazy val getMimeType = WarcRecordUtils.getWarcResponseMimeType(getContentBytes)

  lazy val getUrl = r.getHeader.getUrl

  lazy val getContentString: String = new String(getContentBytes)

  lazy val getContentBytes: Array[Byte] = WarcRecordUtils.getContent(r)
}
