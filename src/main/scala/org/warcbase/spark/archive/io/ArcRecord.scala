package org.warcbase.spark.archive.io

import org.archive.io.arc.ARCRecord
import org.warcbase.data.ArcRecordUtils
import org.warcbase.spark.matchbox.ExtractTopLevelDomain

class ArcRecord(r: ARCRecord) extends ArchiveRecord {
  lazy val getCrawldate: String = r.getMetaData.getDate.substring(0, 8)

  lazy val getDomain: String = ExtractTopLevelDomain(r.getMetaData.getUrl)

  lazy val getMimeType: String = r.getMetaData.getMimetype

  lazy val getUrl: String = r.getMetaData.getUrl

  lazy val getContentString: String = new String(getContentBytes)

  lazy val getContentBytes: Array[Byte] = ArcRecordUtils.getBodyContent(r)
}
