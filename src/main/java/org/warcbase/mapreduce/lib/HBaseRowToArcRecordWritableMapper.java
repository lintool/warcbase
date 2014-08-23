package org.warcbase.mapreduce.lib;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.warcbase.data.ArcRecordUtils;
import org.warcbase.io.ArcRecordWritable;

public class HBaseRowToArcRecordWritableMapper
    extends Mapper<ImmutableBytesWritable, Result, LongWritable, ArcRecordWritable> {
  public static enum Rows { TOTAL };

  private final LongWritable keyOut = new LongWritable();
  private final ArcRecordWritable valueOut = new ArcRecordWritable();

  @Override
  public void map(ImmutableBytesWritable row, Result result, Context context)
      throws IOException, InterruptedException {
    context.getCounter(Rows.TOTAL).increment(1);

    for (KeyValue kv : result.list()) {
      valueOut.setRecord(ArcRecordUtils.fromBytes(kv.getValue()));
      context.write(keyOut, valueOut);
    }
  }
}