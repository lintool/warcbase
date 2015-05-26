package org.warcbase.analysis;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.jwat.warc.WarcRecord;

public class CountWarcContentTypes {
  private static final Logger LOG = Logger.getLogger(CountWarcContentTypes.class);

  private static enum Records { TOTAL };

  public static class MyMapper
      extends Mapper<LongWritable, WarcRecord, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    @Override
    public void map(LongWritable key, WarcRecord record, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      // Implicitly only counts response records (no need to check record.header.warcTypeStr)
      if ((record.getHttpHeader() != null) && (record.getHttpHeader().contentType != null)) {
        context.write(new Text(record.getHttpHeader().contentType.replaceAll(";.*", "")), ONE);
      }
    }
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + CountWarcContentTypes.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    Tool tool = new GenericWarcRecordCounter(MyMapper.class);
    ToolRunner.run(tool, args);
  }
}
