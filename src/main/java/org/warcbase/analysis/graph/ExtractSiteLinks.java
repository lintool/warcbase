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

import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

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
import org.apache.hadoop.mapreduce.Reducer;
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
import org.warcbase.analysis.graph.PrefixMapping.PrefixNode;
import org.warcbase.data.ArcRecordUtils;
import org.warcbase.data.UrlMapping;
import org.warcbase.data.WarcRecordUtils;
import org.warcbase.io.ArcRecordWritable;
import org.warcbase.io.WarcRecordWritable;
import org.warcbase.mapreduce.WacArcInputFormat;
import org.warcbase.mapreduce.WacWarcInputFormat;

public class ExtractSiteLinks extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractSiteLinks.class);

  private static enum Counts {
    RECORDS, HTML_PAGES, LINKS
  };

  public static class ExtractSiteLinksArcMapper extends
      Mapper<LongWritable, ArcRecordWritable, IntWritable, IntWritable> {
    private static final DateFormat df = new SimpleDateFormat("yyyyMMdd");
    private static String beginDate, endDate;
    private static final IntWritable KEY = new IntWritable();
    private static final IntWritable VALUE = new IntWritable();

    private static UrlMapping fst;
    private static PrefixMapping prefixMap;
    private static ArrayList<PrefixNode> prefix;
    
    @Override
    public void setup(Context context) {
      try {
        Configuration conf = context.getConfiguration();
        beginDate = conf.get("beginDate");
        endDate = conf.get("endDate");
        
        @SuppressWarnings("deprecation")
        //Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
        Path[] localFiles = context.getLocalCacheFiles();

        // load FST UriMapping from file
        fst = (UrlMapping) Class.forName(conf.get("UriMappingClass")).newInstance();
        String fstFileName = localFiles[0].toString();
        if (fstFileName.startsWith("file:")) {
          fstFileName = fstFileName.substring(5, fstFileName.length());
        }
        fst.loadMapping(fstFileName);
        // load Prefix Mapping from file
        prefixMap = (PrefixMapping) Class.forName(conf.get("PrefixMappingClass")).newInstance();
        String prefixFileName = localFiles[1].toString();
        if (prefixFileName.startsWith("file:")) {
          prefixFileName = prefixFileName.substring(5, prefixFileName.length());
        }
        prefix = PrefixMapping.loadPrefix(prefixFileName, fst);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error Initializing UriMapping");
      }
    }

    @Override
    public void map(LongWritable key, ArcRecordWritable r, Context context)
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
      
      if (!type.equals("text/html")) {
        return;
      }

      context.getCounter(Counts.HTML_PAGES).increment(1);
      byte[] bytes = ArcRecordUtils.getBodyContent(record);
      Document doc = Jsoup.parse(new String(bytes, "UTF8"), url);

      Elements links = doc.select("a[href]"); // empty if none match
      if (links == null) {
        return;
      }

      int sourcePrefixId = prefixMap.getPrefixId(fst.getID(url), prefix);

      // this url is indexed in FST and its prefix is appeared in prefix map (thus declared in
      // prefix file)
      if (fst.getID(url) != -1 && sourcePrefixId != -1) {
        KEY.set(sourcePrefixId);
        List<Integer> linkUrlList = new ArrayList<Integer>();
        for (Element link : links) {
          String linkUrl = link.attr("abs:href");
          int targetPrefixId = prefixMap.getPrefixId(fst.getID(linkUrl), prefix);
          // target url is indexed in FST and its prefix url is found
          if (fst.getID(linkUrl) != -1 && targetPrefixId != -1) {
            linkUrlList.add(targetPrefixId);
          }
        }

        for (Integer linkID : linkUrlList) {
          VALUE.set(linkID);
          context.write(KEY, VALUE);
        }
      }
    }
  }

  public static class ExtractSiteLinksWarcMapper extends
      Mapper<LongWritable, WarcRecordWritable, IntWritable, IntWritable> {
    private static final DateFormat df = new SimpleDateFormat("yyyyMMdd");
    private static final DateFormat iso8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");
    private static String beginDate, endDate;
    private static final IntWritable KEY = new IntWritable();
    private static final IntWritable VALUE = new IntWritable();

    private static UrlMapping fst;
    private static PrefixMapping prefixMap;
    private static ArrayList<PrefixNode> prefix;

    @Override
    public void setup(Context context) {
      try {
        Configuration conf = context.getConfiguration();
        beginDate = conf.get("beginDate");
        endDate = conf.get("endDate");

        @SuppressWarnings("deprecation")
        //Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
        Path[] localFiles = context.getLocalCacheFiles();

        // load FST UriMapping from file
        fst = (UrlMapping) Class.forName(conf.get("UriMappingClass")).newInstance();
        String fstFileName = localFiles[0].toString();
        if (fstFileName.startsWith("file:")) {
          fstFileName = fstFileName.substring(5, fstFileName.length());
        }
        fst.loadMapping(fstFileName);
        // load Prefix Mapping from file
        prefixMap = (PrefixMapping) Class.forName(conf.get("PrefixMappingClass")).newInstance();
        String prefixFileName = localFiles[1].toString();
        if (prefixFileName.startsWith("file:")) {
          prefixFileName = prefixFileName.substring(5, prefixFileName.length());
        }
        prefix = PrefixMapping.loadPrefix(prefixFileName, fst);
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error Initializing UriMapping");
      }
    }

    @Override
    public void map(LongWritable key, WarcRecordWritable r, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Counts.RECORDS).increment(1);
      WARCRecord record = r.getRecord();
      ArchiveRecordHeader header = record.getHeader();
      byte[] recordBytes = WarcRecordUtils.toBytes(record);
      byte[] content = WarcRecordUtils.getContent(WarcRecordUtils.fromBytes(recordBytes));
      String url = header.getUrl();
      String type = WarcRecordUtils.getWarcResponseMimeType(content);
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

      if (!type.equals("text/html")) {
        return;
      }

      context.getCounter(Counts.HTML_PAGES).increment(1);
      byte[] bytes = WarcRecordUtils.getBodyContent(WarcRecordUtils.fromBytes(recordBytes));
      Document doc = Jsoup.parse(new String(bytes, "UTF8"), url);

      Elements links = doc.select("a[href]"); // empty if none match
      if (links == null) {
        return;
      }

      int sourcePrefixId = prefixMap.getPrefixId(fst.getID(url), prefix);

      // this url is indexed in FST and its prefix is appeared in prefix map (thus declared in
      // prefix file)
      if (fst.getID(url) != -1 && sourcePrefixId != -1) {
        KEY.set(sourcePrefixId);
        List<Integer> linkUrlList = new ArrayList<Integer>();
        for (Element link : links) {
          String linkUrl = link.attr("abs:href");
          int targetPrefixId = prefixMap.getPrefixId(fst.getID(linkUrl), prefix);
          // target url is indexed in FST and its prefix url is found
          if (fst.getID(linkUrl) != -1 && targetPrefixId != -1) {
            linkUrlList.add(targetPrefixId);
          }
        }

        for (Integer linkID : linkUrlList) {
          VALUE.set(linkID);
          context.write(KEY, VALUE);
        }
      }
    }
  }



  private static class ExtractSiteLinksReducer extends
      Reducer<IntWritable, IntWritable, IntWritable, Text> {

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      Int2IntAVLTreeMap links = new Int2IntAVLTreeMap();
      // remove duplicate links
      for (IntWritable value : values) {
        if (links.containsKey(value.get())) {
          // increment 1 link count
          links.put(value.get(), links.get(value.get()) + 1);
        } else {
          links.put(value.get(), 1);
        }
      }

      context.getCounter(Counts.LINKS).increment(links.entrySet().size());
      for (Entry<Integer, Integer> link : links.entrySet()) {
        String outputValue = String.valueOf(link.getKey()) + "," + String.valueOf(link.getValue());
        context.write(key, new Text(outputValue));
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public ExtractSiteLinks() {
  }

  private static final String HDFS = "hdfs";
  private static final String HBASE = "hbase";
  private static final String OUTPUT = "output";
  private static final String URI_MAPPING = "urlMapping";
  private static final String PREFIX_FILE = "prefixFile";
  private static final String NUM_REDUCERS = "numReducers";
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
        .withDescription("prefix mapping file path").create(PREFIX_FILE));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));
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

    if ((!cmdline.hasOption(HDFS) && !cmdline.hasOption(HBASE)) // No HDFS and HBase input
        || !cmdline.hasOption(OUTPUT)
        || !cmdline.hasOption(URI_MAPPING)
        || !cmdline.hasOption(PREFIX_FILE)) {
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
    Path prefixFilePath = new Path(cmdline.getOptionValue(PREFIX_FILE));
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
        .getOptionValue(NUM_REDUCERS)) : 1;
    
    LOG.info("Tool: " + ExtractSiteLinks.class.getSimpleName());
    if (isHDFSInput) {
      LOG.info(" - HDFS input path: " + HDFSPath);
    } else {
      LOG.info(" - HBase table name: " + HBaseTableName);
    }
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - mapping file path:" + mappingPath);
    LOG.info(" - prefix file path:" + prefixFilePath);
    LOG.info(" - number of reducers: " + reduceTasks);
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
    if (!fs.exists(prefixFilePath)) {
      throw new Exception("prefixFilePath doesn't exist: " + prefixFilePath);
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

    Job job = Job.getInstance(conf, ExtractSiteLinks.class.getSimpleName());
    job.setJarByClass(ExtractSiteLinks.class);

    job.getConfiguration().set("UriMappingClass", UrlMapping.class.getCanonicalName());
    job.getConfiguration().set("PrefixMappingClass", PrefixMapping.class.getCanonicalName());
    // Put the mapping file and prefix file in the distributed cache
    // so each map worker will have it.
    job.addCacheFile(mappingPath.toUri());
    job.addCacheFile(prefixFilePath.toUri());

    job.setNumReduceTasks(reduceTasks); // no reducers

    if (isHDFSInput) { // HDFS input
      Path path = new Path(HDFSPath);
      RemoteIterator<LocatedFileStatus> itr = fs.listFiles(path, true);
      LocatedFileStatus fileStatus;
      while (itr.hasNext()) {
        fileStatus = itr.next();
        Path p = fileStatus.getPath();
        if ((p.getName().endsWith(".warc.gz")) || (p.getName().endsWith(".warc"))) {
          // WARC
          MultipleInputs.addInputPath(job, p, WacWarcInputFormat.class, ExtractSiteLinksWarcMapper.class);
        } else {
          // Assume ARC
          MultipleInputs.addInputPath(job, p, WacArcInputFormat.class, ExtractSiteLinksArcMapper.class);
        }
      }

      // set map (key,value) output format
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(IntWritable.class);
    } else { // HBase input
      throw new UnsupportedOperationException("HBase not supported yet!");
    }
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setReducerClass(ExtractSiteLinksReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(job.getConfiguration()).delete(outputDir, true);

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
    ToolRunner.run(new ExtractSiteLinks(), args);
  }
}
