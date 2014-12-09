package org.warcbase.data;

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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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
import org.jwat.warc.WarcRecord;
import org.warcbase.mapreduce.JwatArcInputFormat;
import org.warcbase.mapreduce.JwatWarcInputFormat;

public class UrlMappingMapReduceBuilder extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(UrlMappingMapReduceBuilder.class);

  private static enum Records { TOTAL, UNIQUE };

  public static class ArcUriMappingBuilderMapper extends
      Mapper<LongWritable, ArcRecordBase, Text, Text> {

    public static final Text KEY = new Text();
    public static final Text VALUE = new Text();

    public void map(LongWritable key, ArcRecordBase record, Context context) throws IOException,
        InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);
      if (record.getUrlStr().startsWith("http://")) {
        KEY.set(record.getUrlStr());
        context.write(KEY, VALUE);
      }
    }
  }

  public static class WarcUriMappingBuilderMapper extends
      Mapper<LongWritable, WarcRecord, Text, Text> {

    public static final Text KEY = new Text();
    public static final Text VALUE = new Text();

    public void map(LongWritable key, WarcRecord record, Context context) throws IOException,
        InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);
      String uri = record.header.warcTargetUriStr;
      if ((uri != null) && uri.startsWith("http://")) {
        KEY.set(uri);
        context.write(KEY, VALUE);
      }
    }
  }

  public static class UriMappingBuilderReducer extends
      Reducer<Text, Text, NullWritable, NullWritable> {
    public static List<String> urls = new ArrayList<String>();
    private static String path;

    @Override
    public void setup(Context context) {
      // read PATH variable, which is where to write the FST data
      Configuration conf = context.getConfiguration();
      path = conf.get("PATH");
    }

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.UNIQUE).increment(1);
      urls.add(key.toString());
    }

    @Override
    public void cleanup(Context context) throws IOException {
      PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
      Builder<Long> builder = new Builder<Long>(INPUT_TYPE.BYTE1, outputs);
      BytesRef scratchBytes = new BytesRef();
      IntsRef scratchInts = new IntsRef();
      for (int i = 0; i < urls.size(); i++) {
        if (i % 100000 == 0) {
          LOG.info(i + " URLs processed.");
        }
        scratchBytes.copyChars((String) urls.get(i));
        try {
          builder.add(Util.toIntsRef(scratchBytes, scratchInts), (long) i);
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
        LOG.info("Writing output...");
        fst.save(fstStream);
        success = true;
      } finally {
        if (success) {
          IOUtils.close(fstStream);
          LOG.info("Done!");
        } else {
          IOUtils.closeWhileHandlingException(fstStream);
          LOG.info("Error!");
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

    Path path = new Path(inputPath);
    FileSystem fs = path.getFileSystem(conf);

    RemoteIterator<LocatedFileStatus> itr = fs.listFiles(path, true);
    LocatedFileStatus fileStatus;
    while (itr.hasNext()) {
      fileStatus = itr.next();
      Path p = fileStatus.getPath();
      if ((p.getName().endsWith(".warc.gz")) || (p.getName().endsWith(".warc"))) {
        // WARC
        MultipleInputs.addInputPath(job, p, JwatWarcInputFormat.class, WarcUriMappingBuilderMapper.class);
      } else {
        // Assume ARC
        MultipleInputs.addInputPath(job, p, JwatArcInputFormat.class, ArcUriMappingBuilderMapper.class);
      }
    }

    job.setOutputFormatClass(NullOutputFormat.class); // no output
    // set map (key,value) output format
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setReducerClass(UriMappingBuilderReducer.class);
    // all the keys are shuffled to a single reducer
    job.setNumReduceTasks(1);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Counters counters = job.getCounters();
    LOG.info("Read " + counters.findCounter(Records.TOTAL).getValue() + " total URLs.");
    LOG.info("Read " + counters.findCounter(Records.UNIQUE).getValue() + " unique URLs.");

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new UrlMappingMapReduceBuilder(), args);
  }
}
