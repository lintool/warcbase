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

package org.warcbase.io;

import org.apache.hadoop.io.Writable;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCRecord;
import org.archive.io.warc.WARCRecord;
import org.warcbase.data.ArcRecordUtils;
import org.warcbase.data.WarcRecordUtils;


import java.io.*;

public class GenericArchiveRecordWritable implements Writable {
  public enum ArchiveFormat {UNKNOWN, ARC, WARC}
  private ArchiveFormat format = ArchiveFormat.UNKNOWN;

  private ArchiveRecord record = null;

  public GenericArchiveRecordWritable() {}

  public GenericArchiveRecordWritable(ArchiveRecord r) {
    this.record = r;
    detectFormat();
  }

  public void setRecord(ArchiveRecord r) {
    this.record = r;
    detectFormat();
  }

  public ArchiveRecord getRecord() {
    return record;
  }

  public void detectFormat() {
    if (record instanceof ARCRecord) {
      format = ArchiveFormat.ARC;
    } else if (record instanceof WARCRecord)  {
      format = ArchiveFormat.WARC;
    } else {
      format = ArchiveFormat.UNKNOWN;
    }
  }

  public ArchiveFormat getFormat() {
    return format;
  }

  public void setFormat(ArchiveFormat f) {
    this.format = f;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int len = in.readInt();
    if (len == 0) {
      this.record = null;
      return;
    }

    byte[] bytes = new byte[len];
    in.readFully(bytes);

    if (getFormat() == ArchiveFormat.ARC) {
      this.record = ArcRecordUtils.fromBytes(bytes);
    } else if (getFormat() == ArchiveFormat.WARC) {
      this.record = WarcRecordUtils.fromBytes(bytes);
    } else {
      this.record = null;
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (record == null) {
      out.writeInt(0);
    }
    byte[] bytes;

    if (getFormat() == ArchiveFormat.ARC) {
      bytes = ArcRecordUtils.toBytes((ARCRecord) record);
    } else if (getFormat() == ArchiveFormat.WARC) {
      bytes = WarcRecordUtils.toBytes((WARCRecord) record);
    } else {
      bytes = null;
    }

    out.writeInt(bytes.length);
    out.write(bytes);
  }
}
