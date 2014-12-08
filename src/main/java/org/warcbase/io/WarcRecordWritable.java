package org.warcbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.archive.io.warc.WARCRecord;
import org.warcbase.data.WarcRecordUtils;

public class WarcRecordWritable implements Writable {
  private WARCRecord record = null;

  public WarcRecordWritable() {}

  public WarcRecordWritable(WARCRecord r) {
    this.record = r;
  }

  public void setRecord(WARCRecord r) {
    this.record = r;
  }

  public WARCRecord getRecord() {
    return record;
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

    this.record = WarcRecordUtils.fromBytes(bytes);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (record == null) {
      out.writeInt(0);
    }
    byte[] bytes = WarcRecordUtils.toBytes(record);

    out.writeInt(bytes.length);
    out.write(bytes);
  }
}
