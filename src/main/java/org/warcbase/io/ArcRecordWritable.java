package org.warcbase.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.archive.io.arc.ARCRecord;
import org.warcbase.data.ArcRecordUtils;

public class ArcRecordWritable implements Writable {
  private ARCRecord record = null;

  public ArcRecordWritable() {}

  public ArcRecordWritable(ARCRecord r) {
    this.record = r;
  }

  public void setRecord(ARCRecord r) {
    this.record = r;
  }

  public ARCRecord getRecord() {
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

    this.record = ArcRecordUtils.fromBytes(bytes);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (record == null) {
      out.writeInt(0);
    }
    byte[] bytes = ArcRecordUtils.toBytes(record);

    out.writeInt(bytes.length);
    out.write(bytes);
  }
}
