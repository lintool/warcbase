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
import org.jwat.arc.ArcRecordBase;

public class CountArcCrawlDates {
  private static final Logger LOG = Logger.getLogger(CountArcCrawlDates.class);

  private static enum Records { TOTAL, ERROR };

  public static class MyMapper
      extends Mapper<LongWritable, ArcRecordBase, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    @Override
    public void map(LongWritable key, ArcRecordBase record, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      // Get the crawl day (ignore hour, minute, second)
      String date = record.getArchiveDateStr();
      if ( date.length() < 8) {
        context.getCounter(Records.ERROR).increment(1);
      } else {
        date = date.substring(0, 8);
        context.write(new Text(date), ONE);
      }
    }
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + CountArcCrawlDates.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    Tool tool = new GenericArcRecordCounter(MyMapper.class);
    ToolRunner.run(tool, args);
  }
}
