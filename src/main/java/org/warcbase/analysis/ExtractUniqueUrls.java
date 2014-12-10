package org.warcbase.analysis;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.jwat.arc.ArcRecordBase;
import org.jwat.warc.WarcRecord;
import org.warcbase.mapreduce.JwatArcInputFormat;
import org.warcbase.mapreduce.JwatWarcInputFormat;

public class ExtractUniqueUrls extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractUniqueUrls.class);

  private static enum Records { TOTAL, UNIQUE };

  private static class MyArcMapper
      extends Mapper<LongWritable, ArcRecordBase, Text, IntWritable> {
    private final IntWritable value = new IntWritable(1);
    private final Text url = new Text();

    @Override
    public void map(LongWritable key, ArcRecordBase record, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      if (record.getUrlStr().startsWith("http://")) {
        url.set(record.getUrlStr());
        context.write(url, value);
      }
    }
  }

  private static class MyWarcMapper
      extends Mapper<LongWritable, WarcRecord, Text, IntWritable> {
    private final IntWritable value = new IntWritable(1);
    private final Text url = new Text();

    @Override
    public void map(LongWritable key, WarcRecord record, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);

      String uriStr = record.header.warcTargetUriStr;

      if ((uriStr != null) && uriStr.startsWith("http://")) {
        url.set(uriStr);
        context.write(url, value);
      }
    }
  }

  private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable value = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      value.set(sum);
      context.write(key, value);
      context.getCounter(Records.UNIQUE).increment(1);
    }
  }

  public ExtractUniqueUrls() {}

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

    LOG.info("Tool name: " + ExtractUniqueUrls.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);

    Job job = Job.getInstance(getConf(), ExtractUniqueUrls.class.getSimpleName() + ":" + input);
    job.setJarByClass(ExtractUniqueUrls.class);
    job.setNumReduceTasks(1);

    Path path = new Path(input);
    FileSystem fs = path.getFileSystem(getConf());

    RemoteIterator<LocatedFileStatus> itr = fs.listFiles(path, true);
    LocatedFileStatus fileStatus;
    while (itr.hasNext()) {
      fileStatus = itr.next();
      Path p = fileStatus.getPath();
      if ((p.getName().endsWith(".warc.gz")) || (p.getName().endsWith(".warc"))) {
        // WARC
        MultipleInputs.addInputPath(job, p, JwatWarcInputFormat.class, MyWarcMapper.class);
      } else {
        // Assume ARC
        MultipleInputs.addInputPath(job, p, JwatArcInputFormat.class, MyArcMapper.class);
      }
    }

    FileOutputFormat.setOutputPath(job, output);

    job.setOutputFormatClass(TextOutputFormat.class);

    job.setReducerClass(MyReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    FileSystem ofs = FileSystem.get(getConf());
    if ( FileSystem.get(getConf()).exists(output)) {
      ofs.delete(output, true);
    }

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Counters counters = job.getCounters();
    LOG.info("Read " + counters.findCounter(Records.TOTAL).getValue() + " total URLs.");
    LOG.info("Read " + counters.findCounter(Records.UNIQUE).getValue() + " unique URLs.");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the <code>ToolRunner</code>.
   */
  public static void main(String[] args) throws Exception {
    LOG.info("Running " + ExtractUniqueUrls.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new ExtractUniqueUrls(), args);
  }
}
