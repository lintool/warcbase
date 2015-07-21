package org.warcbase.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import uk.bl.wa.apache.solr.hadoop.Zipper;
import uk.bl.wa.hadoop.ArchiveFileInputFormat;
import uk.bl.wa.hadoop.TextOutputFormat;
import uk.bl.wa.hadoop.indexer.WritableSolrRecord;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

@SuppressWarnings({ "deprecation" })
public class IndexerRunner extends Configured implements Tool {
  public static final String CONFIG_PROPERTIES = "warc_indexer_config";

  private static final Log LOG = LogFactory.getLog(IndexerRunner.class);
  protected static String solrHomeZipName = "solr_home.zip";

  private String inputPath;
  private String outputPath;
  private String configPath;
  private int shards;

  protected void createJobConf(JobConf conf, String[] args) throws IOException, ParseException {
    // Parse the command-line parameters.
    this.setup(args, conf);

    // Store application properties where the mappers/reducers can access them.
    Config config = ConfigFactory.parseFile(new File(this.configPath)); // TODO: make sure config actually exists.
    conf.set(CONFIG_PROPERTIES, config.withOnlyPath("warc").root().render(ConfigRenderOptions.concise()));

    FileSystem fs = FileSystem.get(conf);

    LOG.info("Initializing indexer...");
    String logDir = this.outputPath.trim().replace("/$", "") + ".logs";

    LOG.info("HDFS index output path: " + outputPath);
    conf.set(IndexerReducer.HDFS_OUTPUT_PATH, outputPath);
    if (fs.exists(new Path(outputPath))) {
      LOG.error("Error: path exists already!");
    }

    LOG.info("Logging output: " + logDir);
    FileOutputFormat.setOutputPath(conf, new Path(logDir));
    if (fs.exists(new Path(logDir))) {
      LOG.error("Error: path exists already!");
    }

    int numReducers = this.shards;
    LOG.info("Number of shards: " + shards);
    conf.setInt(IndexerMapper.NUM_SHARDS, shards);

    // Also set reduce speculative execution off, avoiding duplicate submissions to Solr.
    conf.setBoolean("mapreduce.reduce.speculative", false);

    // Add input paths:
    LOG.info("Reading input files...");
    String line = null;
    BufferedReader br = new BufferedReader(new FileReader(this.inputPath));
    while ((line = br.readLine()) != null) {
      FileInputFormat.addInputPath(conf, new Path(line));
    }
    br.close();
    LOG.info("Read " + FileInputFormat.getInputPaths(conf).length + " input files.");

    conf.setJobName(this.inputPath + "_" + System.currentTimeMillis());
    conf.setInputFormat(ArchiveFileInputFormat.class);
    conf.setMapperClass(IndexerMapper.class);
    conf.setReducerClass(IndexerReducer.class);
    conf.setOutputFormat(TextOutputFormat.class);
    // Ensure the JARs we provide take precedence over ones from Hadoop:
    conf.setBoolean("mapreduce.job.user.classpath.first", true);

    cacheSolrHome(conf, solrHomeZipName);

    // Note that we need this to ensure FileSystem.get is thread-safe:
    // @see https://issues.apache.org/jira/browse/HDFS-925
    // @see https://mail-archives.apache.org/mod_mbox/hadoop-user/201208.mbox/%3CCA+4kjVt-QE2L83p85uELjWXiog25bYTKOZXdc1Ahun+oBSJYpQ@mail.gmail.com%3E
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    conf.setMapOutputKeyClass(IntWritable.class);
    conf.setMapOutputValueClass(WritableSolrRecord.class);
    conf.setNumReduceTasks(numReducers);
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

  public int run(String[] args) throws IOException, ParseException {
    // Set up the base conf:
    JobConf conf = new JobConf(getConf(), IndexerRunner.class);

    // Get the job configuration:
    this.createJobConf(conf, args);

    JobClient.runJob(conf);

    return 0;
  }

  private void setup(String[] args, JobConf conf) throws ParseException, IOException {
    // Process Hadoop args first:
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    // Process remaining args list this:
    Options options = new Options();
    options.addOption("i", true, "input file list");
    options.addOption("o", true, "HDFS index output path");
    options.addOption("c", true, "config file");
    options.addOption("s", true, "number of shards");

    CommandLineParser parser = new PosixParser();
    CommandLine cmd = parser.parse(options, otherArgs);
    if (!cmd.hasOption("i") || !cmd.hasOption("o") || !cmd.hasOption("c") || !cmd.hasOption("s")) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp(this.getClass().getName(), options);
      System.exit(1);
    }
    this.inputPath = cmd.getOptionValue("i");
    this.outputPath = cmd.getOptionValue("o");
    this.configPath = cmd.getOptionValue("c");
    this.shards = Integer.parseInt(cmd.getOptionValue("s"));
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new IndexerRunner(), args);
    System.exit(ret);
  }
}
