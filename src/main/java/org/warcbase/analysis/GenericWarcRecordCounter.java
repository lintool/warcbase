package org.warcbase.analysis;

import java.io.IOException;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.jwat.warc.WarcRecord;
import org.warcbase.mapreduce.JwatWarcInputFormat;

public class GenericWarcRecordCounter extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(GenericWarcRecordCounter.class);

  private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private final Class<? extends Mapper<LongWritable, WarcRecord, Text, IntWritable>> mapperClass;

  public GenericWarcRecordCounter(Class<? extends Mapper<LongWritable, WarcRecord, Text, IntWritable>> mapperClass) {
    this.mapperClass = mapperClass;
  }

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

    LOG.info("Tool name: " + CountWarcContentTypes.class.getSimpleName());
    LOG.info(" - input: " + input);
    LOG.info(" - output: " + output);

    Job job = Job.getInstance(getConf(), CountWarcContentTypes.class.getSimpleName() + ":" + input);
    job.setJarByClass(CountWarcContentTypes.class);
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setInputFormatClass(JwatWarcInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(mapperClass);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    FileSystem fs = FileSystem.get(getConf());
    if ( FileSystem.get(getConf()).exists(output)) {
      fs.delete(output, true);
    }

    job.waitForCompletion(true);

    return 0;
  }
}
