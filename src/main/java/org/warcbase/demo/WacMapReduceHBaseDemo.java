package org.warcbase.demo;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;

public class WacMapReduceHBaseDemo extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(WacMapReduceHBaseDemo.class);

  private static enum Records { TOTAL };

  private static class MyMapper extends TableMapper<Text, Text> {
    private final Text KEY = new Text();
    private final Text VALUE = new Text();

    @Override
    public void map(ImmutableBytesWritable row, Result result, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      // set KEY to row key (reversed URL)
      KEY.set(row.get());
      for (KeyValue kv : result.list()) {
        ARCReader reader = (ARCReader) ARCReaderFactory.get(new String(row.get()),
            new BufferedInputStream(new ByteArrayInputStream(kv.getValue())), false);

        ARCRecord record = (ARCRecord) reader.get();
        ARCRecordMetaData meta = record.getMetaData();
        int bodyOffset = record.getBodyOffset();
        String mimeType = new String(kv.getQualifier());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(baos);
        copyStream(record, (int) meta.getLength(), true, dout);

        if (mimeType.startsWith("text")) {
          String content = new String(baos.toByteArray(), "UTF8").substring(bodyOffset)
              .replaceFirst("\\s+", "");
          String excerpt = content.substring(0, Math.min(100, content.length()))
              .replaceAll("[\\n\\r]+", "");
          VALUE.set(mimeType + "\t" + kv.getTimestamp() + "\n" +
              record.getHeaderString() + "\n" + excerpt + "...\n");
        } else {
          VALUE.set(mimeType + "\t" + kv.getTimestamp() + "\n"
              + record.getHeaderString() + "\n");
        }

        context.write(KEY, VALUE);
      }
    }

    protected final byte [] scratchbuffer = new byte[4 * 1024];

    protected long copyStream(final InputStream is, final long recordLength,
        boolean enforceLength, final DataOutputStream out) throws IOException {
      int read = scratchbuffer.length;
      long tot = 0;
      while ((tot < recordLength) && (read = is.read(scratchbuffer)) != -1) {
        int write = read;
        // never write more than enforced length
        write = (int) Math.min(write, recordLength - tot);
        tot += read;
        out.write(scratchbuffer, 0, write);
      }
      if (enforceLength && tot != recordLength) {
        LOG.error("Read " + tot + " bytes but expected " + recordLength + " bytes. Continuing...");
      }

      return tot;
    }

  }

  public WacMapReduceHBaseDemo() {}

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_OPTION = "output";

  /**
   * Runs this tool.
   */
  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT_OPTION));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String input = cmdline.getOptionValue(INPUT_OPTION);
    Path output = new Path(cmdline.getOptionValue(OUTPUT_OPTION));

    LOG.info("Tool name: " + WacMapReduceHBaseDemo.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);

    Configuration config = HBaseConfiguration.create(getConf());
    // This should be fetched from external config files,
    // but not working due to weirdness in current config.
    config.set("hbase.zookeeper.quorum", "bespinrm.umiacs.umd.edu");

    Job job = Job.getInstance(config, WacMapReduceHBaseDemo.class.getSimpleName() + ":" + input);
    job.setJarByClass(WacMapReduceHBaseDemo.class);

    Scan scan = new Scan();
    scan.addFamily("c".getBytes());
    // Very conservative settings because a single row might not fit in memory
    // if we have many captured version of a URL.
    scan.setCaching(1);            // Controls the number of rows to pre-fetch
    scan.setBatch(10);             // Controls the number of columns to fetch on a per row basis
    scan.setCacheBlocks(false);    // Don't set to true for MR jobs
    scan.setMaxVersions();         // We want all versions

    TableMapReduceUtil.initTableMapperJob(
      input,            // input HBase table name
      scan,             // Scan instance to control CF and attribute selection
      MyMapper.class,   // mapper
      Text.class,       // mapper output key
      Text.class,       // mapper output value
      job);

    job.setNumReduceTasks(0);
    FileOutputFormat.setOutputPath(job, output);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileSystem fs = FileSystem.get(getConf());
    if ( FileSystem.get(getConf()).exists(output)) {
      fs.delete(output, true);
    }

    job.waitForCompletion(true);

    Counters counters = job.getCounters();
    int numDocs = (int) counters.findCounter(Records.TOTAL).getValue();
    LOG.info("Read " + numDocs + " rows.");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + WacMapReduceHBaseDemo.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new WacMapReduceHBaseDemo(), args);
  }
}
