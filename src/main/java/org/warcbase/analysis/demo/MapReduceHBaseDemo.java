package org.warcbase.analysis.demo;

import java.io.IOException;
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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class MapReduceHBaseDemo extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(MapReduceHBaseDemo.class);

  private static enum Records { TOTAL };

  private static class MyMapper extends TableMapper<Text, Text> {
    public static final byte[] CF = "c".getBytes();

    @Override
    public void map(ImmutableBytesWritable row, Result result, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      for (KeyValue kv : result.list() ) {
        if (Bytes.equals(kv.getFamily(), CF)) {
          // Key = row (inverse URL)
          // Value = qualifier, timestamp, size
          context.write(new Text(row.get()),
              new Text(new String(kv.getQualifier()) + "\t" + kv.getTimestamp() + "\t" + kv.getValueLength()));
        }
      }
    }
  }

  public MapReduceHBaseDemo() {}

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

    LOG.info("Tool name: " + MapReduceHBaseDemo.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);

    Configuration config = HBaseConfiguration.create(getConf());
    // This should be fetched from external config files,
    // but not working due to weirdness in current config.
    config.set("hbase.zookeeper.quorum", "bespinrm.umiacs.umd.edu");

    Job job = Job.getInstance(config, MapReduceHBaseDemo.class.getSimpleName() + ":" + input);
    job.setJarByClass(MapReduceHBaseDemo.class);

    Scan scan = new Scan();
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
    LOG.info("Read " + numDocs + " records.");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + MapReduceHBaseDemo.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new MapReduceHBaseDemo(), args);
  }
}
