/*
 * Warcbase: an open-source platform for managing web archives
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.warcbase.analysis.graph;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.archive.io.warc.WARCRecord;
import org.archive.util.ArchiveUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.warcbase.data.ArcRecordUtils;
import org.warcbase.data.WarcRecordUtils;
import org.warcbase.data.UrlMapping;
import org.warcbase.io.ArcRecordWritable;
import org.warcbase.io.WarcRecordWritable;
import org.warcbase.mapreduce.WacArcInputFormat;
import org.warcbase.mapreduce.WacWarcInputFormat;

import com.google.common.collect.Lists;

/**
 * Program for extracting links from ARC files or HBase.
 */
public class InvertAnchorText extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(InvertAnchorText.class);

  private static enum Counts {
    RECORDS, HTML_PAGES, LINKS
  };

  private static Int2ObjectMap<List<String>> extractLinks(String content, String url, UrlMapping fst)
      throws IOException {
    Document doc = Jsoup.parse(content, url);
    Elements links = doc.select("a[href]");

    // Note that if there are outgoing links to the same destination page, we retain all copies
    // (and their anchor texts). This behavior is explicitly different from that of ExtractLinks,
    // which de-duplicates outgoing links to the same destination.
    Int2ObjectMap<List<String>> anchors = new Int2ObjectOpenHashMap<List<String>>();
    if (links != null) {
      for (Element link : links) {
        String linkUrl = link.attr("abs:href");
        int id = fst.getID(linkUrl);
        if (id != -1) {
          if (anchors.containsKey(id)) {
            anchors.get(id).add(link.text());
          } else {
            anchors.put(id, Lists.newArrayList(link.text()));
          }
        }
      }
    }

    return anchors;
  }

  public static class InvertAnchorTextArcMapper extends
      Mapper<LongWritable, ArcRecordWritable, IntWritable, Text> {
    private final DateFormat df = new SimpleDateFormat("yyyyMMdd");
    private final IntWritable key = new IntWritable();
    private final Text value = new Text();

    private UrlMapping fst;

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
        fst = (UrlMapping) Class.forName(conf.get("UriMappingClass")).newInstance();
        String fstFileName = localFiles[0].toString();
        if (fstFileName.startsWith("file:")) {
          fstFileName = fstFileName.substring(5, fstFileName.length());
        }
        fst.loadMapping(fstFileName);
        // simply assume only one file in distributed cache.
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error Initializing UriMapping");
      }
    }

    @Override
    public void map(LongWritable k, ArcRecordWritable r, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Counts.RECORDS).increment(1);

      ARCRecord record = r.getRecord();
      ARCRecordMetaData meta = record.getMetaData();
      String url = meta.getUrl();
      String type = meta.getMimetype();
      Date date = null;
      try {
        date = ArchiveUtils.parse14DigitDate(meta.getDate());
      } catch (java.text.ParseException e) {
        e.printStackTrace();
      }

      if (date == null) {
        return;
      }
      String time = df.format(date);

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

      int srcId = -1;
      try {
        srcId = fst.getID(url);
      } catch (Exception e) {
        LOG.error("Error looking up URL: " + url);
        e.printStackTrace();
      }

      if (!type.equals("text/html") || srcId == -1) {
        return;
      }

      context.getCounter(Counts.HTML_PAGES).increment(1);

      byte[] bytes = ArcRecordUtils.getBodyContent(record);
      Int2ObjectMap<List<String>> anchors = InvertAnchorText.extractLinks(new String(bytes, "UTF8"), url, fst);
      for (Int2ObjectMap.Entry<List<String>> entry : anchors.int2ObjectEntrySet()) {
        key.set(entry.getIntKey());
        for (String s : entry.getValue()) {
          value.set(srcId + "\t" + s);
          context.write(key, value);
        }
        context.getCounter(Counts.LINKS).increment(entry.getValue().size());
      }
    }
  }

  public static class InvertAnchorTextWarcMapper extends
      Mapper<LongWritable, WarcRecordWritable, IntWritable, Text> {
    private final DateFormat df = new SimpleDateFormat("yyyyMMdd");
    private final DateFormat iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
    private final IntWritable key = new IntWritable();
    private final Text value = new Text();

    private UrlMapping fst;

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
        fst = (UrlMapping) Class.forName(conf.get("UriMappingClass")).newInstance();
        String fstFileName = localFiles[0].toString();
        if (fstFileName.startsWith("file:")) {
          fstFileName = fstFileName.substring(5, fstFileName.length());
        }
        fst.loadMapping(fstFileName);
        // simply assume only one file in distributed cache.
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error Initializing UriMapping");
      }
    }

    @Override
    public void map(LongWritable k, WarcRecordWritable r, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Counts.RECORDS).increment(1);

      WARCRecord record = r.getRecord();
      ArchiveRecordHeader header = record.getHeader();
      byte[] recordBytes;
      byte[] content;
      String url;
      String type;

      // Corrupt records can cause these methods to throw OOM exceptions: catch and ignore record.
      try {
        recordBytes = WarcRecordUtils.toBytes(record);
        content = WarcRecordUtils.getContent(WarcRecordUtils.fromBytes(recordBytes));
        url = header.getUrl();
        type = WarcRecordUtils.getWarcResponseMimeType(content);
      } catch (java.lang.OutOfMemoryError e) {
        LOG.error("Caught OutOfMemoryError, skipping record.");
        return;
      }

      if (type == null) type = "";
      Date date = null;
      try {
        date = iso8601.parse(header.getDate());
      } catch (java.text.ParseException e) {
        e.printStackTrace();
      }

      if (date == null) {
        return;
      }
      String time = df.format(date);

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

      int srcId = -1;
      try {
        srcId = fst.getID(url);
      } catch (Exception e) {
        LOG.error("Error looking up URL: " + url);
        e.printStackTrace();
      }

      if (!type.equals("text/html") || srcId == -1) {
        return;
      }

      context.getCounter(Counts.HTML_PAGES).increment(1);

      byte[] bytes;
      try {
        bytes = WarcRecordUtils.getBodyContent(WarcRecordUtils.fromBytes(recordBytes));
      } catch (Exception e) {
        LOG.error(e.getMessage() + ": skipping record.");
        return;
      } catch (java.lang.OutOfMemoryError e) {
        LOG.error("Caught OutOfMemoryError, skipping record.");
        return;
      }

      Int2ObjectMap<List<String>> anchors = InvertAnchorText.extractLinks(new String(bytes, "UTF8"), url, fst);
      for (Int2ObjectMap.Entry<List<String>> entry : anchors.int2ObjectEntrySet()) {
        key.set(entry.getIntKey());
        for (String s : entry.getValue()) {
          value.set(srcId + "\t" + s);
          context.write(key, value);
        }
        context.getCounter(Counts.LINKS).increment(entry.getValue().size());
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public InvertAnchorText() {}

  private static final String HDFS = "hdfs";
  private static final String HBASE = "hbase";
  private static final String OUTPUT = "output";
  private static final String URI_MAPPING = "urlMapping";
  private static final String BEGIN = "begin";
  private static final String END = "end";
  private static final String NUM_REDUCERS = "numReducers";

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
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

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
    String HDFSPath = null, table = null;
    boolean isHdfs;
    if (cmdline.hasOption(HDFS)) {
      HDFSPath = cmdline.getOptionValue(HDFS);
      isHdfs = true;
    } else {
      table = cmdline.getOptionValue(HBASE);
      isHdfs = false;
    }
    String outputPath = cmdline.getOptionValue(OUTPUT);
    Path mappingPath = new Path(cmdline.getOptionValue(URI_MAPPING));

    LOG.info("Tool: " + InvertAnchorText.class.getSimpleName());
    if (isHdfs) {
      LOG.info(" - HDFS input path: " + HDFSPath);
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

    Job job = Job.getInstance(conf, InvertAnchorText.class.getSimpleName() +
        (isHdfs ? ":HDFS:" + HDFSPath : ":HBase:" + table));
    job.setJarByClass(InvertAnchorText.class);

    job.getConfiguration().set("UriMappingClass", UrlMapping.class.getCanonicalName());
    // Put the mapping file in the distributed cache so each map worker will have it.
    job.addCacheFile(mappingPath.toUri());

    int numReducers = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 100;
    job.setNumReduceTasks(numReducers);

    if (isHdfs) { // HDFS input
      Path path = new Path(HDFSPath);
      RemoteIterator<LocatedFileStatus> itr = fs.listFiles(path, true);
      LocatedFileStatus fileStatus;
      while (itr.hasNext()) {
        fileStatus = itr.next();
        Path p = fileStatus.getPath();
        if ((p.getName().endsWith(".warc.gz")) || (p.getName().endsWith(".warc"))) {
          // WARC
          MultipleInputs.addInputPath(job, p, WacWarcInputFormat.class, InvertAnchorTextWarcMapper.class);
        } else {
          // Assume ARC
          MultipleInputs.addInputPath(job, p, WacArcInputFormat.class, InvertAnchorTextArcMapper.class);
        }
      }

      // set map (key,value) output format
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);
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
    ToolRunner.run(new InvertAnchorText(), args);
  }
}
