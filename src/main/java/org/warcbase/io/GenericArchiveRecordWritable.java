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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GenericArchiveRecordWritable implements Writable {
  private ArchiveRecord record = null;

  public GenericArchiveRecordWritable() {}

  public GenericArchiveRecordWritable(ArchiveRecord r) {
    this.record = r;
  }

  public void setRecord(ArchiveRecord r) {
    this.record = r;
  }

  public ArchiveRecord getRecord() {
    return record;
  }

  public String getFormat() {
    if (record instanceof ARCRecord) {
      return "ARC";
    } else if (record instanceof WARCRecord)  {
      return "WARC";
    } else {
      return "Unknown";
    }
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

    if (record instanceof ARCRecord) {
      this.record = ArcRecordUtils.fromBytes(bytes);
    } else {
      this.record = WarcRecordUtils.fromBytes(bytes);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (record == null) {
      out.writeInt(0);
    }
    byte[] bytes;
    if (record instanceof ARCRecord) {
      bytes = ArcRecordUtils.toBytes((ARCRecord) record);
    } else {
      bytes = WarcRecordUtils.toBytes((WARCRecord) record);
    }

    out.writeInt(bytes.length);
    out.write(bytes);
  }
}
