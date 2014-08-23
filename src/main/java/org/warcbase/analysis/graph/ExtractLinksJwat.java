package org.warcbase.analysis.graph;

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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jwat.arc.ArcRecordBase;
import org.warcbase.data.UrlMapping;
import org.warcbase.mapreduce.JwatArcInputFormat;

import com.google.common.base.Joiner;

/**
 * Program for extracting links from ARC files.
 */
public class ExtractLinksJwat extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractLinksJwat.class);
  
  private static enum Counts {
    RECORDS, LINKS, HTML_PAGES
  };

  public static class ExtractLinksHdfsMapper extends
      Mapper<LongWritable, ArcRecordBase, IntWritable, Text> {
    private final Joiner joiner = Joiner.on(",");
    private final IntWritable outKey = new IntWritable();
    private final Text outValue = new Text();
    
    private final DateFormat df = new SimpleDateFormat("yyyyMMdd");
    private UrlMapping fst;
    private String beginDate, endDate;

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
        fst = (UrlMapping) Class.forName(conf.get("UriMappingClass")).newInstance();
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
      context.getCounter(Counts.RECORDS).increment(1);
      String url = record.getUrlStr();
      String type = record.getContentTypeStr();
      Date date = record.getArchiveDate();
      if (date == null) {
        return;
      }
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

      if (!type.equals("text/html")) {
        return;
      }

      if (fst.getID(url) == -1) {
        return;
      }

      context.getCounter(Counts.HTML_PAGES).increment(1);

      Document doc = Jsoup.parse(content, "ISO-8859-1", url);
      Elements links = doc.select("a[href]");

      if (links == null) {
        return;
      }
      
      outKey.set(fst.getID(url));
      IntAVLTreeSet linkUrlSet = new IntAVLTreeSet();
      for (Element link : links) {
        String linkUrl = link.attr("abs:href");
        if (fst.getID(linkUrl) != -1) { // link already exists
          linkUrlSet.add(fst.getID(linkUrl));
        }
      }

      if (linkUrlSet.size() == 0) {
        // Emit empty entry even if there aren't any outgoing links
        outValue.set("");
        context.write(outKey, outValue);
        return;
      }

      outValue.set(joiner.join(linkUrlSet));
      context.getCounter(Counts.LINKS).increment(linkUrlSet.size());
      context.write(outKey, outValue);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public ExtractLinksJwat() {}

  private static final String HDFS = "hdfs";
  private static final String HBASE = "hbase";
  private static final String OUTPUT = "output";
  private static final String URI_MAPPING = "urlMapping";
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
    String HDFSPath = null, HBaseTableName = null;
    boolean isHDFSInput = true; // set default as HDFS input
    if (cmdline.hasOption(HDFS)) {
      HDFSPath = cmdline.getOptionValue(HDFS);
    } else {
      HBaseTableName = cmdline.getOptionValue(HBASE);
      isHDFSInput = false;
    }
    String outputPath = cmdline.getOptionValue(OUTPUT);
    Path mappingPath = new Path(cmdline.getOptionValue(URI_MAPPING));

    LOG.info("Tool: " + ExtractLinksJwat.class.getSimpleName());
    if (isHDFSInput) {
      LOG.info(" - HDFS input path: " + HDFSPath);
    } else {
      LOG.info(" - HBase table name: " + HBaseTableName);
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
    if (isHDFSInput) {
      conf = getConf();
      // passing global variable values to individual nodes
      if(beginDate != null) {
        conf.set("beginDate", beginDate);
      }
      if(endDate != null) {
        conf.set("endDate", endDate);
      }
    } else {
      conf = HBaseConfiguration.create(getConf());
      conf.set("hbase.zookeeper.quorum", "bespinrm.umiacs.umd.edu");
    }
      
    Job job = Job.getInstance(conf, ExtractLinksJwat.class.getSimpleName());
    job.setJarByClass(ExtractLinksJwat.class);

    job.getConfiguration().set("UriMappingClass", UrlMapping.class.getCanonicalName());
    // Put the mapping file in the distributed cache so each map worker will have it.
    job.addCacheFile(mappingPath.toUri());

    job.setNumReduceTasks(0); // no reducers
    
    if (isHDFSInput) { // HDFS input
      FileInputFormat.setInputPaths(job, new Path(HDFSPath));
  
      job.setInputFormatClass(JwatArcInputFormat.class);
      // set map (key,value) output format
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);
  
      job.setMapperClass(ExtractLinksHdfsMapper.class);
    } else { // HBase input
      throw new UnsupportedOperationException("HBase not supported yet!");
    }
    
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    fs.delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    Counters counters = job.getCounters();
    LOG.info("Read " + counters.findCounter(Counts.RECORDS).getValue() + " records.");
    LOG.info("Processed " + counters.findCounter(Counts.HTML_PAGES).getValue() + " HTML pages.");
    LOG.info("Extracted " + counters.findCounter(Counts.LINKS).getValue() + " links.");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExtractLinksJwat(), args);
  }
}