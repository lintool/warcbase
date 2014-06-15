package org.warcbase.analysis.graph;

import it.unimi.dsi.fastutil.ints.Int2IntAVLTreeMap;

import java.io.IOException;
import java.io.InputStream;
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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
import org.warcbase.analysis.graph.PrefixMapping.PrefixNode;
import org.warcbase.data.UriMapping;
import org.warcbase.mapreduce.ArcInputFormat;

public class ExtractSiteLinks extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractSiteLinks.class);

  private static enum Records {
    TOTAL, LINK_COUNT
  };

  public static class ExtractSiteLinksMapper extends
      Mapper<LongWritable, ArcRecordBase, IntWritable, IntWritable> {
    private static final DateFormat df = new SimpleDateFormat("yyyyMMdd");
    private static final IntWritable KEY = new IntWritable();
    private static final IntWritable VALUE = new IntWritable();

    private static UriMapping fst;
    private static PrefixMapping prefixMap;
    private static List<PrefixNode> prefix;

    @Override
    public void setup(Context context) {
      try {
        Configuration conf = context.getConfiguration();
        @SuppressWarnings("deprecation")
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        // load FST UriMapping from file
        fst = (UriMapping) Class.forName(conf.get("UriMappingClass")).newInstance();
        fst.loadMapping(localFiles[0].toString());
        // load Prefix Mapping from file
        prefixMap = (PrefixMapping) Class.forName(conf.get("PrefixMappingClass")).newInstance();
        prefix = PrefixMapping.loadPrefix(localFiles[1].toString(), fst);

      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Error Initializing UriMapping");
      }
    }

    @Override
    public void map(LongWritable key, ArcRecordBase record, Context context)
        throws IOException, InterruptedException {
      context.getCounter(Records.TOTAL).increment(1);
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

      if (!type.equals("text/html")) {
        return;
      }

      Document doc = Jsoup.parse(content, "ISO-8859-1", url); // parse in ISO-8859-1 format
      Elements links = doc.select("a[href]"); // empty if none match
      if (links == null) {
        return;
      }
      
      int sourcePrefixId = prefixMap.getPrefixId(fst.getID(url), prefix);
      
      // this url is indexed in FST and its prefix is appeared in prefix map (thus declared in prefix file) 
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
     } // end map function
  } 

  private static class ExtractSiteLinksReducer extends
      Reducer<IntWritable, IntWritable, IntWritable, Text> {
    private static final Text VALUE = new Text();

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
      
      context.getCounter(Records.LINK_COUNT).increment(links.entrySet().size());
      for (Entry<Integer, Integer> link : links.entrySet()) {
        VALUE.set(link.getKey() + "\t" + link.getValue());
        context.write(key, VALUE);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public ExtractSiteLinks() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String URI_MAPPING = "uriMapping";
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
        .withDescription("input path").create(INPUT));
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

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(URI_MAPPING)
        || !cmdline.hasOption(PREFIX_FILE)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    FileSystem fs = FileSystem.get(getConf());

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    Path mappingPath = new Path(cmdline.getOptionValue(URI_MAPPING));
    Path prefixFilePath = new Path(cmdline.getOptionValue(PREFIX_FILE));
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
        .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + ExtractSiteLinks.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
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

    Job job = Job.getInstance(getConf(), ExtractSiteLinks.class.getSimpleName());
    job.setJarByClass(ExtractSiteLinks.class);

    job.getConfiguration().set("UriMappingClass", UriMapping.class.getCanonicalName());
    job.getConfiguration().set("PrefixMappingClass", PrefixMapping.class.getCanonicalName());
    // Put the mapping file and prefix file in the distributed cache
    // so each map worker will have it.
    job.addCacheFile(mappingPath.toUri());
    job.addCacheFile(prefixFilePath.toUri());

    job.setNumReduceTasks(reduceTasks); // no reducers

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(ArcInputFormat.class);
    // set map (key,value) output format
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(ExtractSiteLinksMapper.class);
    job.setReducerClass(ExtractSiteLinksReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(job.getConfiguration()).delete(outputDir, true);

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
    ToolRunner.run(new ExtractSiteLinks(), args);
  }
}
