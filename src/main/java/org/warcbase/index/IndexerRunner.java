package org.warcbase.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.bl.wa.apache.solr.hadoop.Zipper;
import uk.bl.wa.hadoop.ArchiveFileInputFormat;
import uk.bl.wa.hadoop.indexer.WritableSolrRecord;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

@SuppressWarnings({ "deprecation" })
public class IndexerRunner extends Configured implements Tool {
  public static final String CONFIG_PROPERTIES = "IndexerRunner.Config";

  private static final Log LOG = LogFactory.getLog(IndexerRunner.class);
  protected static String solrHomeZipName = "solr_home.zip";

  public static final String INPUT_OPTION = "input";
  public static final String INDEX_OPTION = "index";
  public static final String CONFIG_OPTION = "config";
  public static final String SHARDS_OPTION = "numShards";

  @SuppressWarnings("static-access")
  public int run(String[] args) throws IOException, ParseException {
    LOG.info("Initializing indexer...");

    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("file").hasArg()
        .withDescription("input file list").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("HDFS index output path").create(INDEX_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of shards").create(SHARDS_OPTION));
    options.addOption(OptionBuilder.withArgName("file").hasArg()
        .withDescription("config file (optional)").create(CONFIG_OPTION));

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

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(INDEX_OPTION) || !cmdline.hasOption(SHARDS_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String configPath = null;
    if (cmdline.hasOption(CONFIG_OPTION)) {
      configPath = cmdline.getOptionValue(CONFIG_OPTION);
    }

    String inputPath = cmdline.getOptionValue(INPUT_OPTION);
    String outputPath = cmdline.getOptionValue(INDEX_OPTION);
    int shards = Integer.parseInt(cmdline.getOptionValue(SHARDS_OPTION));

    JobConf conf = new JobConf(getConf(), IndexerRunner.class);

    if (configPath == null) {
      LOG.info("Config not specified, using default src/main/solr/WARCIndexer.conf");
      configPath = "src/main/solr/WARCIndexer.conf";
    }
    File configFile = new File(configPath);
    if (!configFile.exists()) {
      LOG.error("Error: config does not exist!");
      System.exit(-1);
    }
    Config config = ConfigFactory.parseFile(configFile);
    conf.set(CONFIG_PROPERTIES, config.withOnlyPath("warc").root().render(ConfigRenderOptions.concise()));

    FileSystem fs = FileSystem.get(conf);

    LOG.info("HDFS index output path: " + outputPath);
    conf.set(IndexerReducer.HDFS_OUTPUT_PATH, outputPath);
    if (fs.exists(new Path(outputPath))) {
      LOG.error("Error: path exists already!");
      System.exit(-1);
    }

    LOG.info("Number of shards: " + shards);
    conf.setInt(IndexerMapper.NUM_SHARDS, shards);

    // Add input paths:
    LOG.info("Reading input files...");
    String line = null;
    BufferedReader br = new BufferedReader(new FileReader(inputPath));
    while ((line = br.readLine()) != null) {
      FileInputFormat.addInputPath(conf, new Path(line));
    }
    br.close();
    LOG.info("Read " + FileInputFormat.getInputPaths(conf).length + " input files.");

    conf.setJobName(IndexerRunner.class.getSimpleName() + ": " + inputPath);
    conf.setInputFormat(ArchiveFileInputFormat.class);
    conf.setMapperClass(IndexerMapper.class);
    conf.setReducerClass(IndexerReducer.class);
    conf.setOutputFormat(NullOutputFormat.class);

    // Ensure the JARs we provide take precedence over ones from Hadoop:
    conf.setBoolean("mapreduce.job.user.classpath.first", true);
    // Also set reduce speculative execution off, avoiding duplicate submissions to Solr.
    conf.setBoolean("mapreduce.reduce.speculative", false);

    // Note that we need this to ensure FileSystem.get is thread-safe:
    // @see https://issues.apache.org/jira/browse/HDFS-925
    // @see https://mail-archives.apache.org/mod_mbox/hadoop-user/201208.mbox/%3CCA+4kjVt-QE2L83p85uELjWXiog25bYTKOZXdc1Ahun+oBSJYpQ@mail.gmail.com%3E
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(WritableSolrRecord.class);
    conf.setNumReduceTasks(shards);  // number of reducers = number of shards

    cacheSolrHome(conf, solrHomeZipName);

    JobClient.runJob(conf);

    return 0;
  }

  private void cacheSolrHome(JobConf conf, String solrHomeZipName) throws IOException {
    File tmpSolrHomeDir = new File("src/main/solr").getAbsoluteFile();

    // Create a ZIP file.
    File solrHomeLocalZip = File.createTempFile("tmp-", solrHomeZipName);
    Zipper.zipDir(tmpSolrHomeDir, solrHomeLocalZip);

    // Add to HDFS.
    FileSystem fs = FileSystem.get(conf);
    String hdfsSolrHomeDir = fs.getHomeDirectory() + "/solr/tempHome/" + solrHomeZipName;
    fs.copyFromLocalFile(new Path(solrHomeLocalZip.toString()), new Path(hdfsSolrHomeDir));

    final URI baseZipUrl = fs.getUri().resolve(hdfsSolrHomeDir + '#' + solrHomeZipName);

    // Cache it.
    DistributedCache.addCacheArchive(baseZipUrl, conf);
  }

  public static void main(String[] args) throws Exception {
    LOG.info("Running " + IndexerRunner.class.getCanonicalName() + " with args "
        + Arrays.toString(args));
    ToolRunner.run(new IndexerRunner(), args);
  }
}
