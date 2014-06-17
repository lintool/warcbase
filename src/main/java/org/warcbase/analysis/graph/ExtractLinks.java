package org.warcbase.analysis.graph;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jwat.arc.ArcRecordBase;
import org.warcbase.data.UriMapping;
import org.warcbase.data.Util;
import org.warcbase.mapreduce.ArcInputFormat;

import com.google.common.base.Joiner;

/**
 * Program for extracting links from ARC files or HBase.
 */
public class ExtractLinks extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractLinks.class);

  private static enum MyCounters {
    RECORDS, HTML_PAGES, LINKS
  };

  private static IntSortedSet extractLinks(InputStream content, String url, UriMapping fst)
      throws IOException {
    Document doc = Jsoup.parse(content, "ISO-8859-1", url); // parse in ISO-8859-1 format
    Elements links = doc.select("a[href]");

    IntSortedSet linkDestinations = new IntAVLTreeSet();
    if (links != null) {
      for (Element link : links) {
        String linkUrl = link.attr("abs:href");
        if (fst.getID(linkUrl) != -1) {
          // Note, we explicitly de-duplicate outgoing pages to the same target.
          linkDestinations.add(fst.getID(linkUrl));
        }
      }
    }

    return linkDestinations;
  }

  public static class ExtractLinksHdfsMapper extends
      Mapper<LongWritable, ArcRecordBase, IntWritable, Text> {
    private final DateFormat df = new SimpleDateFormat("yyyyMMdd");
    private final Joiner joiner = Joiner.on(",");
    private final IntWritable key = new IntWritable();
    private final Text value = new Text();

    private UriMapping fst;

    @Override
    public void setup(Context context) {
      try {
        Configuration conf = context.getConfiguration();
        // There appears to be a bug in getCacheFiles() which returns null,
        // even though getLocalCacheFiles is deprecated...
        @SuppressWarnings("deprecation")
        Path[] localFiles = context.getLocalCacheFiles();

        LOG.info("cache contents: " + Arrays.toString(localFiles));
        System.out.println("cache contents: " + Arrays.toString(localFiles));

        // load FST UriMapping from file
        fst = (UriMapping) Class.forName(conf.get("UriMappingClass")).newInstance();
        fst.loadMapping(localFiles[0].toString());
        // simply assume only one file in distributed cache.
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error Initializing UriMapping");
      }
    }

    @Override
    public void map(LongWritable k, ArcRecordBase record, Context context)
        throws IOException, InterruptedException {
      context.getCounter(MyCounters.RECORDS).increment(1);

      String url = record.getUrlStr();
      String type = record.getContentTypeStr();
      Date date = record.getArchiveDate();
      String time = df.format(date);
      InputStream content = record.getPayloadContent();

      if (beginDate != null && endDate != null) {
        if (time.compareTo(beginDate) < 0 || time.compareTo(endDate) > 0) {
          return;
        }
      } else if (beginDate == null && endDate != null) {
        if (time.compareTo(endDate) > 0) {
          return;
        }
      } else if (beginDate != null && endDate == null) {
        if (time.compareTo(beginDate) < 0) {
          return;
        }
      }

      int id = fst.getID(url);
      if (!type.equals("text/html") || id == -1) {
        return;
      }

      context.getCounter(MyCounters.HTML_PAGES).increment(1);

      IntSortedSet linkDestinations = ExtractLinks.extractLinks(content, url, fst);

      key.set(id);
      if (linkDestinations.size() == 0) {
        // Emit empty entry even if there aren't any outgoing links
        value.set("");
        context.write(key, value);
      } else {
        value.set(joiner.join(linkDestinations));
        context.write(key, value);
        context.getCounter(MyCounters.LINKS).increment(linkDestinations.size());
      }
    }
  }
  
  public static class ExtractLinksHBaseMapper extends TableMapper<IntWritable, Text>{
    private final Joiner joiner = Joiner.on(",");
    private final IntWritable key = new IntWritable();
    private final Text value = new Text();
    
    private UriMapping fst;

    @Override
    public void setup(Context context) {
      try {
        Configuration conf = context.getConfiguration();
        @SuppressWarnings("deprecation")
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        // load FST UriMapping from file
        fst = (UriMapping) Class.forName(conf.get("UriMappingClass")).newInstance();
        fst.loadMapping(localFiles[0].toString());
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error Initializing UriMapping");
      }
    }
    
    @Override
    public void map(ImmutableBytesWritable row, Result result, Context context)
        throws IOException, InterruptedException {

      String url = Util.reverseBacUri(new String(row.get()));

      int srcId = fst.getID(url);
      if ( srcId == -1) {
        return;
      }
      
      key.set(srcId);
      for (KeyValue kv : result.list()) {
        String type = new String(kv.getQualifier());

        context.getCounter(MyCounters.RECORDS).increment(1);

        if (!type.equals("text/html")) {
          continue;
        }

        context.getCounter(MyCounters.HTML_PAGES).increment(1);

        InputStream content = new ByteArrayInputStream(kv.getValue());
        IntSortedSet linkDestinations = ExtractLinks.extractLinks(content, url, fst);

        if (linkDestinations.size() == 0) {
          // Emit empty entry even if there aren't any outgoing links
          value.set("");
          context.write(key, value);
        } else {
          value.set(joiner.join(linkDestinations));
          context.write(key, value);
          context.getCounter(MyCounters.LINKS).increment(linkDestinations.size());
        }
      }
    }
  }
  
  /**
   * Creates an instance of this tool.
   */
  public ExtractLinks() {}

  private static final String HDFS = "hdfs";
  private static final String HBASE = "hbase";
  private static final String OUTPUT = "output";
  private static final String URI_MAPPING = "uriMapping";
  private static final String BEGIN = "begin";
  private static final String END = "end";
  private static String beginDate = null, endDate = null;

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("HDFS input path").create(HDFS));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("HBASE table name").create(HBASE));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("uri mapping file path").create(URI_MAPPING));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("begin date (optional)").create(BEGIN));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("end date (optional)").create(END));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if ( (!cmdline.hasOption(HDFS) && !cmdline.hasOption(HBASE)) // No HDFS and HBase input
        || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(URI_MAPPING)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    FileSystem fs = FileSystem.get(getConf());
    String path = null, table = null;
    boolean isHdfs;
    if (cmdline.hasOption(HDFS)) {
      path = cmdline.getOptionValue(HDFS);
      isHdfs = true;
    } else {
      table = cmdline.getOptionValue(HBASE);
      isHdfs = false;
    }
    String outputPath = cmdline.getOptionValue(OUTPUT);
    Path mappingPath = new Path(cmdline.getOptionValue(URI_MAPPING));

    LOG.info("Tool: " + ExtractLinks.class.getSimpleName());
    if (isHdfs) {
      LOG.info(" - HDFS input path: " + path);
    } else {
      LOG.info(" - HBase table name: " + table);
    }
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - mapping file path: " + mappingPath);

    if (cmdline.hasOption(BEGIN)) {
      beginDate = cmdline.getOptionValue(BEGIN);
      LOG.info(" - begin date: " + beginDate);
    }
    if (cmdline.hasOption(END)) {
      endDate = cmdline.getOptionValue(END);
      LOG.info(" - end date: " + endDate);
    }

    if (!fs.exists(mappingPath)) {
      throw new Exception("mappingPath doesn't exist: " + mappingPath);
    }
    
    Configuration conf;
    if (isHdfs) {
      conf = getConf();
    } else {
      conf = HBaseConfiguration.create(getConf());
      conf.set("hbase.zookeeper.quorum", "bespinrm.umiacs.umd.edu");
    }
      
    Job job = Job.getInstance(conf, ExtractLinks.class.getSimpleName() +
        (isHdfs ? ":HDFS:" + path : ":HBase:" + table));
    job.setJarByClass(ExtractLinks.class);

    job.getConfiguration().set("UriMappingClass", UriMapping.class.getCanonicalName());
    // Put the mapping file in the distributed cache so each map worker will have it.
    job.addCacheFile(mappingPath.toUri());

    job.setNumReduceTasks(0); // no reducers
    
    if (isHdfs) { // HDFS input
      FileInputFormat.setInputPaths(job, new Path(path));
  
      job.setInputFormatClass(ArcInputFormat.class);
      // set map (key,value) output format
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);
  
      job.setMapperClass(ExtractLinksHdfsMapper.class);
    } else { // HBase input
      Scan scan = new Scan();
      scan.addFamily("c".getBytes());
      // Very conservative settings because a single row might not fit in memory
      // if we have many captured version of a URL.
      scan.setCaching(1);            // Controls the number of rows to pre-fetch
      scan.setBatch(10);             // Controls the number of columns to fetch on a per row basis
      scan.setCacheBlocks(false);    // Don't set to true for MR jobs
      scan.setMaxVersions();         // We want all versions

      TableMapReduceUtil.initTableMapperJob(
        table,                  // input HBase table name
        scan,                            // Scan instance to control CF and attribute selection
        ExtractLinksHBaseMapper.class,   // mapper 
        IntWritable.class,               // mapper output key
        Text.class,                      // mapper output value
        job);
      job.setOutputFormatClass(TextOutputFormat.class); // set output format
    }
    
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    fs.delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Counters counters = job.getCounters();
    int numRecords = (int) counters.findCounter(MyCounters.RECORDS).getValue();
    int numLinks = (int) counters.findCounter(MyCounters.LINKS).getValue();
    LOG.info("Read " + numRecords + " records.");
    LOG.info("Extracts " + numLinks + " links.");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExtractLinks(), args);
  }
}