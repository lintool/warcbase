package org.warcbase.demo;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.warcbase.io.ArcRecordWritable;
import org.warcbase.mapreduce.WacArcInputFormat;

public class WacMapReduceArcDemo extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(WacMapReduceArcDemo.class);

  public static enum Records { TOTAL };

  public static class MyMapper
      extends Mapper<LongWritable, ArcRecordWritable, Text, Text> {
    @Override
    public void map(LongWritable key, ArcRecordWritable r, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      ARCRecord record = r.getRecord();
      ARCRecordMetaData meta = record.getMetaData();
      String url = meta.getUrl();
      String date = meta.getDate();
      String type = meta.getMimetype();

      context.write(new Text(url + " " + type), new Text(date));
    }
  }

  public WacMapReduceArcDemo() {}

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

    LOG.info("Tool name: " + WacMapReduceArcDemo.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);

    Job job = Job.getInstance(getConf(), WacMapReduceArcDemo.class.getSimpleName() + ":" + input);
    job.setJarByClass(WacMapReduceArcDemo.class);
    job.setNumReduceTasks(0);

    FileInputFormat.addInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.setInputFormatClass(WacArcInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapperClass(MyMapper.class);

    FileSystem fs = FileSystem.get(getConf());
    if ( FileSystem.get(getConf()).exists(output)) {
      fs.delete(output, true);
    }

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Counters counters = job.getCounters();
    int numDocs = (int) counters.findCounter(Records.TOTAL).getValue();
    LOG.info("Read " + numDocs + " records.");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + WacMapReduceArcDemo.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new WacMapReduceArcDemo(), args);
  }
}
