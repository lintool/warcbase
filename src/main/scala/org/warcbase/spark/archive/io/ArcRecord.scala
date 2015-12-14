package org.warcbase.spark.archive.io

import org.apache.spark.SerializableWritable
import org.warcbase.data.ArcRecordUtils
import org.warcbase.io.ArcRecordWritable
import org.warcbase.spark.matchbox.ExtractTopLevelDomain

class ArcRecord(r: SerializableWritable[ArcRecordWritable]) extends ArchiveRecord {
  val getCrawldate: String = r.t.getRecord.getMetaData.getDate.substring(0, 8)

  val getMimeType: String = r.t.getRecord.getMetaData.getMimetype

  val getUrl: String = r.t.getRecord.getMetaData.getUrl

  val getDomain: String = ExtractTopLevelDomain(r.t.getRecord.getMetaData.getUrl)

  val getContentBytes: Array[Byte] = ArcRecordUtils.getBodyContent(r.t.getRecord)

  val getContentString: String = new String(getContentBytes)

}
