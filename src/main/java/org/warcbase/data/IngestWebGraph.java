package org.warcbase.data;

import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;

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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
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
import org.warcbase.mapreduce.JwatArcInputFormat;
import org.warcbase.data.UrlMapping;

import com.google.common.base.Joiner;

public class IngestWebGraph extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(IngestWebGraph.class);

  private static enum Records {
    TOTAL, LINK_COUNT
  };

  public static class IngestWebGraphHDFSMapper extends
      Mapper<LongWritable, ArcRecordBase, Text, Text> {
    private static final Joiner JOINER = Joiner.on(",");
    public static final Text KEY = new Text();
    private static final Text VALUE = new Text();

    private static final DateFormat df = new SimpleDateFormat("yyyyMMdd");
    private static UrlMapping fst;
    private static String beginDate, endDate;

    @Override
    public void setup(Context context) {
      try {
        Configuration conf = context.getConfiguration();
        beginDate = conf.get("beginDate");
        endDate = conf.get("endDate");
        // There appears to be a bug in getCacheFiles() which returns null,
        // even though getLocalCacheFiles is deprecated...
        @SuppressWarnings("deprecation")
        Path[] localFiles = context.getLocalCacheFiles();

        LOG.info("cache contents: " + Arrays.toString(localFiles));
        System.out.println("cache contents: " + Arrays.toString(localFiles));

        // load FST UriMapping from file
        fst = (UrlMapping) Class.forName(conf.get("UrlMappingClass")).newInstance();
        fst.loadMapping(localFiles[0].toString());
        // simply assume only one file in distributed cache.

      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error Initializing UriMapping");
      }
    }

    @Override
    public void map(LongWritable key, ArcRecordBase record, Context context) throws IOException,
        InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);
      String url = record.getUrlStr();
      String type = record.getContentTypeStr();
      Date date = record.getArchiveDate();
      if (date == null) {
        return;
      }
      String time = df.format(date);
      long epoch = date.getTime();

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

      if (!type.equals("text/html")) {
        return;
      }

      Document doc = Jsoup.parse(content, "ISO-8859-1", url); // parse in ISO-8859-1 format
      Elements links = doc.select("a[href]"); // empty if none match

      if (fst.getID(url) != -1) { // the url is already indexed in UriMapping
        KEY.set(url + "," + epoch);
        IntAVLTreeSet linkUrlSet = new IntAVLTreeSet();
        if (links != null) {
          for (Element link : links) {
            String linkUrl = link.attr("abs:href");
            if (fst.getID(linkUrl) != -1) { // link already exists
              linkUrlSet.add(fst.getID(linkUrl));
            }
          }

          if (linkUrlSet.size() == 0) {
            // Emit empty entry even if there aren't any outgoing links
            VALUE.set("");
            context.write(KEY, VALUE);
            return;
          }

          VALUE.set(JOINER.join(linkUrlSet));
          context.getCounter(Records.LINK_COUNT).increment(linkUrlSet.size());
          context.write(KEY, VALUE);
        }
      }
    }
  }

  public static class IngestWebGraphHBaseReducer extends
      TableReducer<Text, Text, ImmutableBytesWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
        InterruptedException {
      LOG.info(key.toString() + " " + new String(key.getBytes()));
      String[] groups = key.toString().split(",");
      String sourceUrl = groups[0];
      String timestamp = groups[1];
      String reverseUrl = reversedUrl(sourceUrl);
      Put p = new Put(reverseUrl.getBytes());
      for (Text target_ids : values) {
        p.add(CF, timestamp.getBytes(), target_ids.getBytes());
      }
      context.write(null, p);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public static String reversedUrl(String url) {
    // url: http://www.bbc.com/dir/index.php
    // hostname: www.bbc.com
    int beginIndex = url.indexOf("://");
    String tmp = url.replace("://", "");
    int endIndex = tmp.indexOf("/");
    String hostname = tmp.substring(beginIndex, endIndex);
    String[] arr = hostname.split("\\.");
    String reverseHostName = "";
    for (int i = arr.length - 1; i > 0; i--) {
      reverseHostName += arr[i] + ".";
    }
    reverseHostName += arr[0];
    return url.replace(hostname, reverseHostName);
  }

  public static void CreateTable(Configuration conf, String tableName) throws IOException,
      ZooKeeperConnectionException {
    HBaseAdmin hbase = new HBaseAdmin(conf);
    HTableDescriptor[] wordcounts = hbase.listTables(tableName);

    if (wordcounts.length != 0) { // Drop Table if Exists
      hbase.disableTable(tableName);
      hbase.deleteTable(tableName);
    }

    HTableDescriptor wordcount = new HTableDescriptor(tableName);
    hbase.createTable(wordcount);
    // Cannot edit a stucture on an active table.
    hbase.disableTable(tableName);
    HColumnDescriptor columnFamily = new HColumnDescriptor(CF);
    hbase.addColumn(tableName, columnFamily);
    hbase.enableTable(tableName);

    hbase.close();
  }

  public IngestWebGraph() {}

  public static final byte[] CF = "links".getBytes();

  private static final String HDFS = "hdfs";
  private static final String OUTPUT = "output";
  private static final String URI_MAPPING = "uriMapping";
  private static final String NUM_REDUCERS = "numReducers";
  private static final String BEGIN = "begin";
  private static final String END = "end";
  private static String beginDate = null, endDate = null;
  private static int DEFAULT_NUM_REDUCERS = 10;

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("HDFS input path").create(HDFS));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("uri mapping file path").create(URI_MAPPING));
    options.addOption(OptionBuilder.withArgName("argument").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));
    options.addOption(OptionBuilder.withArgName("argument").hasArg()
        .withDescription("begin date (optional)").create(BEGIN));
    options.addOption(OptionBuilder.withArgName("argument").hasArg()
        .withDescription("end date (optional)").create(END));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(HDFS) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(URI_MAPPING)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    FileSystem fs = FileSystem.get(getConf());
    String HDFSPath = cmdline.getOptionValue(HDFS);
    String HBaseTableName = cmdline.getOptionValue(OUTPUT);
    Path mappingPath = new Path(cmdline.getOptionValue(URI_MAPPING));

    LOG.info("Tool: " + IngestWebGraph.class.getSimpleName());
    LOG.info(" - HDFS input path: " + HDFSPath);
    LOG.info(" - HBase output path: " + HBaseTableName);
    LOG.info(" - mapping file path: " + mappingPath);

    int numReducers = DEFAULT_NUM_REDUCERS;
    if (cmdline.hasOption(NUM_REDUCERS)) {
      numReducers = Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS));
      LOG.info(" - number of reducers: " + numReducers);
    }
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

    Configuration conf = HBaseConfiguration.create(getConf());
    conf.set("hbase.zookeeper.quorum", "bespinrm.umiacs.umd.edu");
    // passing global variable values to individual nodes
    if (beginDate != null) {
      conf.set("beginDate", beginDate);
    }
    if (endDate != null) {
      conf.set("endDate", endDate);
    }
    CreateTable(conf, HBaseTableName);
    Job job = Job.getInstance(conf, IngestWebGraph.class.getSimpleName());
    job.setJarByClass(IngestWebGraph.class);

    job.getConfiguration().set("UrlMappingClass", UrlMapping.class.getCanonicalName());
    // Put the mapping file in the distributed cache so each map worker will have it.
    job.addCacheFile(mappingPath.toUri());

    FileInputFormat.setInputPaths(job, new Path(HDFSPath));

    job.setInputFormatClass(JwatArcInputFormat.class);
    // set map (key,value) output format
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setNumReduceTasks(numReducers);

    job.setMapperClass(IngestWebGraphHDFSMapper.class);
    // set HBase Reducer output
    TableMapReduceUtil.initTableReducerJob(HBaseTableName, // output table
        IngestWebGraphHBaseReducer.class, // reducer class
        job);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Counters counters = job.getCounters();
    int numRecords = (int) counters.findCounter(Records.TOTAL).getValue();
    int numLinks = (int) counters.findCounter(Records.LINK_COUNT).getValue();
    LOG.info("Read " + numRecords + " records.");
    LOG.info("Extracts " + numLinks + " links.");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new IngestWebGraph(), args);
  }
}
