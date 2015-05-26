package org.warcbase.analysis;

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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.jwat.warc.WarcRecord;
import org.warcbase.mapreduce.JwatWarcInputFormat;

public class FindWarcUrls extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(FindWarcUrls.class);

  private static enum Records { TOTAL };

  private static class MyMapper extends Mapper<LongWritable, WarcRecord, Text, Text> {
    private static final Text KEY = new Text();
    private static final Text VALUE = new Text();
    private String pattern = null;

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      pattern = conf.get(PATTERN_OPTION);
    }

    @Override
    public void map(LongWritable key, WarcRecord record, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      // Only consider response records
      if (record.header.warcTypeStr.equalsIgnoreCase("response")) {
        String uriStr = record.header.warcTargetUriStr;
        String date = record.header.warcDateStr;
        String type = "";
        if (record.getHttpHeader() != null) {
          if (record.getHttpHeader().contentType != null) {
            type = record.getHttpHeader().contentType.replaceAll(";.*", "");
          }
        }

        if ((uriStr != null) && uriStr.matches(pattern)) {
          String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
          KEY.set(fileName + " " + uriStr + " " + type);
          VALUE.set(date);
          context.write(KEY, VALUE);
        }
      }
    }
  }

  public FindWarcUrls() {}

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_OPTION = "output";
  public static final String PATTERN_OPTION = "pattern";

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
    options.addOption(OptionBuilder.withArgName("regexp").hasArg()
        .withDescription("URL pattern").create(PATTERN_OPTION));

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
    String pattern = cmdline.getOptionValue(PATTERN_OPTION);

    LOG.info("Tool name: " + FindWarcUrls.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);

    Job job = Job.getInstance(getConf(), FindWarcUrls.class.getSimpleName() + ":" + input);
    job.setJarByClass(FindWarcUrls.class);
    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    FileInputFormat.addInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.setInputFormatClass(JwatWarcInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setMapperClass(MyMapper.class);

    job.getConfiguration().set(PATTERN_OPTION, pattern);

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
    LOG.info("Running " + FindWarcUrls.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new FindWarcUrls(), args);
  }
}
