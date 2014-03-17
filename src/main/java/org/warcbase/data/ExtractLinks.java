package org.warcbase.data;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.jsoup.HttpStatusException;
import org.jsoup.Jsoup;
import org.jsoup.UnsupportedMimeTypeException;
import org.jsoup.helper.Validate;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.jwat.arc.ArcRecordBase;
import org.jwat.common.HttpHeader;
import org.warcbase.mapreduce.ArcInputFormat;
import java.util.Arrays;

/**
 * Extract Links demo.
 * 
 * @author Jinfeng Rao modified code based on CountTrecDocuments.java by Jimmy Lin
 */
public class ExtractLinks extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractLinks.class);

  private static enum Records {
    TOTAL, LINK_COUNT
  };

  public static class ExtractLinksMapper extends
      Mapper<LongWritable, ArcRecordBase, IntWritable, List> {
    private IntWritable urlNode = new IntWritable();
    private List linkNodes;
    private static UriMapping fst;

    @Override
    public void setup(Context context) {
      try {
        Configuration conf = context.getConfiguration();
        Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

        // load FST UriMapping from file
        fst = (UriMapping) Class.forName(conf.get("UriMappingClass")).newInstance();
        fst.loadMapping(localFiles[0].toString());// simply assume only one file in distributed
                                                  // cache
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
      InputStream content = record.getPayloadContent();

      if (!type.equals("text/html"))
        return;
      Document doc = Jsoup.parse(content, "ISO-8859-1", url); // parse in ISO-8859-1 format
      Elements links = doc.select("a[href]"); // empty if none match

      if (fst.getID(url) != -1) { // the url is already indexed in UriMapping
        urlNode.set(fst.getID(url));
        linkNodes = new ArrayList<IntWritable>();
        Set<IntWritable> linkUrlSet = new HashSet<IntWritable>();
        if (links != null) {
          for (Element link : links) {
            String linkUrl = link.attr("abs:href");
            if (fst.getID(linkUrl) != -1) { // linkUrl is already indexed
              linkUrlSet.add(new IntWritable(fst.getID(linkUrl)));
            }
          }
          boolean emitFlag = false;
          for (IntWritable linkID : linkUrlSet) {
            linkNodes.add(linkID);
            emitFlag = true;
            context.getCounter(Records.LINK_COUNT).increment(1);
          }
          if (emitFlag == false) { // contain no links which are indexed in UriMapping
            context.getCounter(Records.LINK_COUNT).increment(1);
          }

        } else { // webpage without outgoing links
          context.getCounter(Records.LINK_COUNT).increment(1);
        }
        context.write(urlNode, linkNodes);
      }
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public ExtractLinks() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String URI_MAPPING = "uriMapping";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path")
        .create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path")
        .create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("uri mapping file path").create(URI_MAPPING));
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

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT) || !cmdline.hasOption(URI_MAPPING)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    String mappingPath = cmdline.getOptionValue(URI_MAPPING);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline
        .getOptionValue(NUM_REDUCERS)) : 1;

    LOG.info("Tool: " + ExtractLinks.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - mapping file path:" + mappingPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Job job = new Job(getConf(), ExtractLinks.class.getSimpleName());
    job.setJarByClass(ExtractLinks.class);

    // Pass in the class name as a String; this is makes the mapper general
    // in being able to load any collection of Indexable objects that has
    // url_id/url mapping specified by a UriMapping object.
    job.getConfiguration().set("UriMappingClass", UriMapping.class.getCanonicalName());
    // Put the mapping file in the distributed cache so each map worker will
    // have it.
    DistributedCache.addCacheFile(new URI(mappingPath), job.getConfiguration());

    job.setNumReduceTasks(0); // no reducers

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setInputFormatClass(ArcInputFormat.class);
    // set map (key,value) output format
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(List.class);

    job.setMapperClass(ExtractLinksMapper.class);

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
    ToolRunner.run(new ExtractLinks(), args);
  }
}