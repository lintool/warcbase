package org.warcbase.spark.matchbox

import java.text.SimpleDateFormat

import org.archive.io.arc.ARCRecord
import org.archive.io.warc.WARCRecord
import org.archive.util.ArchiveUtils
import org.warcbase.data.{ArcRecordUtils, WarcRecordUtils}

/**
  * Provides a common interface from which ARCRecords and WARCRecords can be accessed.
  *
  * Specifically, a WARecord has fields crawldate, url, domain, mimeType, and bodyContent.
  */
object RecordTransformers {

  trait WARecord extends Serializable {
    def getCrawldate: String

    def getUrl: String

    def getDomain: String

    def getMimeType: String

    def getBodyContent: String

    def getRawBodyContent: String
  }

  implicit def toArcRecord(r: ARCRecord): WARecord = new ArcRecord(r)

  implicit def toWarcRecord(r: WARCRecord): WARecord = new WarcRecord(r)

  class ArcRecord(r: ARCRecord) extends WARecord {
    override def getCrawldate: String = r.getMetaData.getDate.substring(0, 8)

    override def getDomain: String = ExtractTopLevelDomain(r.getMetaData.getUrl)

    override def getMimeType: String = r.getMetaData.getMimetype

    override def getUrl: String = r.getMetaData.getUrl

    override def getBodyContent: String = new String(ArcRecordUtils.getBodyContent(r))

    override def getRawBodyContent: String = ExtractRawText(new String(ArcRecordUtils.getBodyContent(r)))
  }

  class WarcRecord(r: WARCRecord) extends WARecord {
    val ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")
    private val content = WarcRecordUtils.getContent(r)

    override def getCrawldate: String = ArchiveUtils.get14DigitDate(ISO8601.parse(r.getHeader.getDate)).substring(0, 8)

    override def getDomain = ExtractTopLevelDomain(r.getHeader.getUrl).replace("^\\s*www\\.", "")

    override def getMimeType = WarcRecordUtils.getWarcResponseMimeType(content)

    override def getUrl = r.getHeader.getUrl

    override def getBodyContent: String = new String(content)

    override def getRawBodyContent: String = ExtractRawText(new String(WarcRecordUtils.getBodyContent(r)))
  }

}
