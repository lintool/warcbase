package org.warcbase.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
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
import org.apache.zookeeper.KeeperException;

import uk.bl.wa.apache.solr.hadoop.Solate;
import uk.bl.wa.hadoop.ArchiveFileInputFormat;
import uk.bl.wa.hadoop.TextOutputFormat;
import uk.bl.wa.hadoop.indexer.WritableSolrRecord;
import uk.bl.wa.solr.SolrWebServer;
import uk.bl.wa.util.ConfigPrinter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;

/**
 * WARCIndexerRunner
 * 
 * Extracts text/metadata using from a series of Archive files.
 * 
 * @author rcoram
 */

@SuppressWarnings({ "deprecation" })
public class WARCIndexerRunner extends Configured implements Tool {
	private static final Log LOG = LogFactory.getLog(WARCIndexerRunner.class);
	private static final String CLI_USAGE = "[-i <input file>] [-o <output dir>] [-c <config file>] [-d] [Dump config.] [-w] [Wait for completion.] [-x] [output XML in OAI-PMH format]";
	private static final String CLI_HEADER = "WARCIndexerRunner - MapReduce method for extracing metadata/text from Archive Records";
	public static final String CONFIG_PROPERTIES = "warc_indexer_config";
	public static final String CONFIG_APPLY_ANNOTATIONS = "webarchive.indexer.applyAnnotations";

	protected static String solrHomeZipName = "solr_home.zip";

	private String inputPath;
	private String outputPath;
	private String configPath;
	private boolean wait;
	private boolean dumpConfig;
	private boolean exportXml;
	private boolean applyAnnotations;

	/**
	 * 
	 * @param args
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	protected void createJobConf(JobConf conf, String[] args)
			throws IOException, ParseException, KeeperException,
			InterruptedException {
		// Parse the command-line parameters.
		this.setup(args, conf);

		// Store application properties where the mappers/reducers can access
		// them
		Config index_conf;
		if (this.configPath != null) {
			index_conf = ConfigFactory.parseFile(new File(this.configPath));
		} else {
			index_conf = ConfigFactory.load();
		}
		if (this.dumpConfig) {
			ConfigPrinter.print(index_conf);
			System.exit(0);
		}
		conf.set(CONFIG_PROPERTIES, index_conf.withOnlyPath("warc").root()
				.render(ConfigRenderOptions.concise()));
		LOG.info("Loaded warc config.");
		LOG.info(index_conf.getString("warc.title"));
		if (index_conf.getBoolean("warc.solr.use_hash_url_id")) {
			LOG.info("Using hash-based ID.");
		}
		if (index_conf.hasPath("warc.solr.zookeepers")) {
			LOG.info("Using Zookeepers.");
		} else {
			LOG.info("Using SolrServers.");
		}

		// Also set reduce speculative execution off, avoiding duplicate
		// submissions to Solr.
		conf.set("mapred.reduce.tasks.speculative.execution", "false");

		// Reducer count dependent on concurrent HTTP connections to Solr
		// server.
		int numReducers = 1;
		try {
			numReducers = index_conf.getInt("warc.hadoop.num_reducers");
		} catch (NumberFormatException n) {
			numReducers = 10;
		}

		// Add input paths:
		LOG.info("Reading input files...");
		String line = null;
		BufferedReader br = new BufferedReader(new FileReader(this.inputPath));
		while ((line = br.readLine()) != null) {
			FileInputFormat.addInputPath(conf, new Path(line));
		}
		br.close();
		LOG.info("Read " + FileInputFormat.getInputPaths(conf).length
				+ " input files.");

		FileOutputFormat.setOutputPath(conf, new Path(this.outputPath));

		conf.setJobName(this.inputPath + "_" + System.currentTimeMillis());
		conf.setInputFormat(ArchiveFileInputFormat.class);
		conf.setMapperClass(WARCIndexerMapper.class);
		conf.setReducerClass(WARCIndexerReducer.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.set("map.output.key.field.separator", "");
		// Compress the output from the maps, to cut down temp space
		// requirements between map and reduce.
		conf.setBoolean("mapreduce.map.output.compress", true); // Wrong syntax
		// for 0.20.x ?
		conf.set("mapred.compress.map.output", "true");
		// conf.set("mapred.map.output.compression.codec",
		// "org.apache.hadoop.io.compress.GzipCodec");
		// Ensure the JARs we provide take precedence over ones from Hadoop:
		conf.setBoolean("mapreduce.task.classpath.user.precedence", true);

		// If we are indexing into HDFS, we need a copy of the config:
		if (index_conf.getBoolean("warc.solr.hdfs")) {
			// Grab the Solr config from ZK and cache it for during the job.
//			if (index_conf.hasPath(SolrWebServer.CONF_ZOOKEEPERS)) {
//				Solate.cacheSolrHome(conf,
//						index_conf.getString(SolrWebServer.CONF_ZOOKEEPERS),
//						index_conf.getString(SolrWebServer.COLLECTION),
//						solrHomeZipName);
//			} else {
				Solate.cacheSolrHome(conf, null, null, solrHomeZipName);
//			}
			// TODO Check num_shards == num reducers

			// Note that we need this to ensure FileSystem.get is thread-safe:
			// @see https://issues.apache.org/jira/browse/HDFS-925
			// @see
			// https://mail-archives.apache.org/mod_mbox/hadoop-user/201208.mbox/%3CCA+4kjVt-QE2L83p85uELjWXiog25bYTKOZXdc1Ahun+oBSJYpQ@mail.gmail.com%3E
			conf.setBoolean("fs.hdfs.impl.disable.cache", true);
		}
		conf.setBoolean("mapred.output.oai-pmh", this.exportXml);

		// Decide whether to apply annotations:
		conf.setBoolean(CONFIG_APPLY_ANNOTATIONS, this.applyAnnotations);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(WritableSolrRecord.class);
		conf.setNumReduceTasks(numReducers);
	}

	/**
	 * 
	 * Run the job:
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 * 
	 */
	public int run(String[] args) throws IOException, ParseException,
			KeeperException, InterruptedException {
		// Set up the base conf:
		JobConf conf = new JobConf(getConf(), WARCIndexerRunner.class);

		// Get the job configuration:
		this.createJobConf(conf, args);

		// Submit it:
		if (this.wait) {
			JobClient.runJob(conf);
		} else {
			JobClient client = new JobClient(conf);
			client.submitJob(conf);
		}
		return 0;
	}

	private void setup(String[] args, JobConf conf) throws ParseException, IOException {
		// Process Hadoop args first:
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();

		// Process remaining args list this:
		Options options = new Options();
		options.addOption("i", true, "input file list");
		options.addOption("o", true, "output directory");
		options.addOption("c", true, "path to configuration");
		options.addOption("w", false, "wait for job to finish");
		options.addOption("d", false, "dump configuration");
		options.addOption("x", false, "output XML in OAI-PMH format");
		options.addOption("a", false,
				"apply annotations found via '-files annotations.json'");
		// TODO: Problematic with "hadoop jar"?
		// I think starting with the GenericOptionsParser (above) should resolve
		// this?
		// options.addOption( OptionBuilder.withArgName( "property=value"
		// ).hasArgs( 2 ).withValueSeparator().withDescription(
		// "use value for given property" ).create( "D" ) );

		CommandLineParser parser = new PosixParser();
		CommandLine cmd = parser.parse(options, otherArgs);
		if (!cmd.hasOption("i") || !cmd.hasOption("o")) {
			HelpFormatter helpFormatter = new HelpFormatter();
			helpFormatter.setWidth(80);
			helpFormatter.printHelp(CLI_USAGE, CLI_HEADER, options, "");
			System.exit(1);
		}
		this.inputPath = cmd.getOptionValue("i");
		this.outputPath = cmd.getOptionValue("o");
		this.wait = cmd.hasOption("w");
		if (cmd.hasOption("c")) {
			this.configPath = cmd.getOptionValue("c");
		}
		this.dumpConfig = cmd.hasOption("d");
		this.exportXml = cmd.hasOption("x");
		this.applyAnnotations = cmd.hasOption("a");
	}

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new WARCIndexerRunner(), args);
		System.exit(ret);
	}

}
