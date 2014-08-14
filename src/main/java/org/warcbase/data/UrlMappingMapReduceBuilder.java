package org.warcbase.data;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.INPUT_TYPE;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.jwat.arc.ArcRecordBase;
import org.warcbase.mapreduce.ArcInputFormat;

public class UrlMappingMapReduceBuilder extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(UrlMappingMapReduceBuilder.class);

  private static enum Records {
    TOTAL, RECORD_COUNT
  };

  public static class UriMappingBuilderMapper extends
      Mapper<LongWritable, ArcRecordBase, Text, Text> {

    public static final Text KEY = new Text();
    public static final Text VALUE = new Text();

    public void map(LongWritable key, ArcRecordBase record, Context context) throws IOException,
        InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);
      String url = record.getUrlStr();
      String type = record.getContentTypeStr();

      if (!type.equals("text/html")) {
        return;
      }

      KEY.set(url);
      context.write(KEY, VALUE);
    }
  }

  public static class UriMappingBuilderReducer extends
      Reducer<Text, Text, NullWritable, NullWritable> {
    public static List<String> urls = new ArrayList<String>();
    private static String path;

    // read PATH environment
    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      path = conf.get("PATH");
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.RECORD_COUNT).increment(1);
      urls.add(key.toString());
    }

    @Override
    public void cleanup(Context context) throws IOException {
      long size = urls.size();
      LongList outputValues = new LongArrayList(); // create the mapping id

      for (long i = 1; i <= size; i++) {
        outputValues.add(i);
      }

      PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
      Builder<Long> builder = new Builder<Long>(INPUT_TYPE.BYTE1, outputs);
      BytesRef scratchBytes = new BytesRef();
      IntsRef scratchInts = new IntsRef();
      for (int i = 0; i < size; i++) {
        scratchBytes.copyChars((String) urls.get(i));
        try {
          builder.add(Util.toIntsRef(scratchBytes, scratchInts), (Long) outputValues.get(i));
        } catch (UnsupportedOperationException e) {
          LOG.error("Duplicate URL:" + urls.get(i));
        } catch (IOException e) {
          LOG.error(e.getMessage());
          e.printStackTrace();
        }
      }
      FST<Long> fst = builder.finish();

      LOG.info("PATH: " + path);
      // Delete the output directory if it exists already.
      Path outputDir = new Path(path);
      FileSystem.get(context.getConfiguration()).delete(outputDir, true);

      // Save FST to file
      FileSystem fs = FileSystem.get(context.getConfiguration());
      Path fstPath = new Path(path);
      OutputStream fStream = fs.create(fstPath);
      OutputStreamDataOutput fstStream = new OutputStreamDataOutput(fStream);
      boolean success = false;
      try {
        fst.save(fstStream);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(fstStream);
        } else {
          IOUtils.closeWhileHandlingException(fstStream);
        }
      }
    }
  }

  public UrlMappingMapReduceBuilder() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path")
        .hasArg().withDescription("output path").create(OUTPUT));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);

    LOG.info("- input path: " + inputPath);
    LOG.info("- output path: " + outputPath);

    Configuration conf = getConf();
    conf.set("PATH", outputPath);
    conf.set("mapreduce.reduce.java.opts", "-Xmx5120m");
    Job job = Job.getInstance(conf, UrlMappingMapReduceBuilder.class.getSimpleName());
    job.setJarByClass(UrlMappingMapReduceBuilder.class);

    job.getConfiguration().set("UriMappingBuilderClass",
        UrlMappingMapReduceBuilder.class.getCanonicalName());

    FileInputFormat.setInputPaths(job, new Path(inputPath));

    job.setInputFormatClass(ArcInputFormat.class);
    job.setOutputFormatClass(NullOutputFormat.class); // no output
    // set map (key,value) output format
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setMapperClass(UriMappingBuilderMapper.class);
    job.setReducerClass(UriMappingBuilderReducer.class);
    // all the keys are shuffled to a single reducer
    job.setNumReduceTasks(1);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Counters counters = job.getCounters();
    int numRecords = (int) counters.findCounter(Records.TOTAL).getValue();
    int numUrls = (int) counters.findCounter(Records.RECORD_COUNT).getValue();
    LOG.info("Read " + numRecords + " records.");
    LOG.info("Encountered " + numUrls + " unique urls.");

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new UrlMappingMapReduceBuilder(), args);
  }
}
