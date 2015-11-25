/*
 * Warcbase: an open-source platform for managing web archives
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    def getContentString: String

    def getContentBytes: Array[Byte]
  }

  implicit def toArcRecord(r: ARCRecord): WARecord = new ArcRecord(r)

  implicit def toWarcRecord(r: WARCRecord): WARecord = new WarcRecord(r)

  class ArcRecord(r: ARCRecord) extends WARecord {
    override def getCrawldate: String = r.getMetaData.getDate.substring(0, 8)

    override def getDomain: String = ExtractTopLevelDomain(r.getMetaData.getUrl)

    override def getMimeType: String = r.getMetaData.getMimetype

    override def getUrl: String = r.getMetaData.getUrl

    override def getContentString: String = new String(getContentBytes)

    override def getContentBytes: Array[Byte] = ArcRecordUtils.getBodyContent(r)
  }

  class WarcRecord(r: WARCRecord) extends WARecord {
    val ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")

    override def getCrawldate: String = ArchiveUtils.get14DigitDate(ISO8601.parse(r.getHeader.getDate)).substring(0, 8)

    override def getDomain = ExtractTopLevelDomain(getUrl).replace("^\\s*www\\.", "")

    override def getMimeType = WarcRecordUtils.getWarcResponseMimeType(getContentBytes)

    override def getUrl = r.getHeader.getUrl

    override def getContentString: String = new String(getContentBytes)

    override def getContentBytes: Array[Byte] = WarcRecordUtils.getBodyContent(r)
  }

}
