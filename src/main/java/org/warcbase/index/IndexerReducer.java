package org.warcbase.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.HdfsDirectoryFactory;

import uk.bl.wa.apache.solr.hadoop.Solate;
import uk.bl.wa.hadoop.indexer.MetadataBuilder;
import uk.bl.wa.hadoop.indexer.WritableSolrRecord;
import uk.bl.wa.solr.SolrFields;
import uk.bl.wa.solr.SolrRecord;
import uk.bl.wa.solr.SolrWebServer;
import uk.bl.wa.solr.WctEnricher;
import uk.bl.wa.solr.WctFields;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class IndexerReducer extends MapReduceBase implements
    Reducer<IntWritable, WritableSolrRecord, Text, Text> {
  private static final Log LOG = LogFactory.getLog(IndexerReducer.class);
  // TODO: make the shard/partition terminology consistent
  private static final String SHARD_PREFIX = "shard";

  private SolrServer solrServer;
  private int batchSize;
  private List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
  private int numberOfSequentialFails = 0;
  private static final int SUBMISSION_PAUSE_MINS = 5;

  private FileSystem fs;
  private Path solrHomeDir = null;
  private Path outputDir;
  private boolean exportXml = false;

  static enum MyCounters {
    NUM_RECORDS, NUM_ERRORS, NUM_DROPPED_RECORDS
  }

  /**
   * Sets up our SolrServer. Presumes the existence of either "warc.solr.zookepers" or
   * "warc.solr.servers" in the config.
   */
  @Override
  public void configure(JobConf job) {
    LOG.info("Configuring reducer, including Solr connection...");

    // Get config from job property:
    Config conf = ConfigFactory.parseString(job.get(IndexerRunner.CONFIG_PROPERTIES));

    this.batchSize = conf.getInt("warc.solr.batch_size");

    if (job.get("mapred.output.oai-pmh") != null)
      this.exportXml = Boolean.parseBoolean(job.get("mapred.output.oai-pmh"));

    // Initialize the embedded server.
    try {
      job.setBoolean("fs.hdfs.impl.disable.cache", true);
      fs = FileSystem.get(job);
      solrHomeDir = Solate.findSolrConfig(job, IndexerRunner.solrHomeZipName);
      LOG.info("Found solrHomeDir " + solrHomeDir);
    } catch (IOException e) {
      e.printStackTrace();
      LOG.error("FAILED in reducer configuration: " + e);
    }
    outputDir = new Path(conf.getString(SolrWebServer.HDFS_OUTPUT_PATH));

    LOG.info("Initialisation complete.");
  }

  private void initEmbeddedServer(int slice) throws IOException {
    if (solrHomeDir == null) {
      throw new IOException("Unable to find solr home setting");
    }

    Path outputShardDir = new Path(fs.getHomeDirectory() + "/" + outputDir, SHARD_PREFIX + slice);

    LOG.info("Creating embedded Solr server with solrHomeDir: " + solrHomeDir + ", fs: " + fs
        + ", outputShardDir: " + outputShardDir);

    Path solrDataDir = new Path(outputShardDir, "data");
    if (!fs.exists(solrDataDir) && !fs.mkdirs(solrDataDir)) {
      throw new IOException("Unable to create " + solrDataDir);
    }

    String dataDirStr = solrDataDir.toUri().toString();
    LOG.info("Attempting to set data dir to: " + dataDirStr);

    System.setProperty("solr.data.dir", dataDirStr);
    System.setProperty("solr.home", solrHomeDir.toString());
    System.setProperty("solr.solr.home", solrHomeDir.toString());
    System.setProperty("solr.hdfs.home", outputDir.toString());
    System.setProperty("solr.directoryFactory", HdfsDirectoryFactory.class.getName());
    System.setProperty("solr.lock.type", "hdfs");
    System.setProperty("solr.hdfs.nrtcachingdirectory", "false");
    System.setProperty("solr.hdfs.blockcache.enabled", "true");
    System.setProperty("solr.hdfs.blockcache.write.enabled", "false");
    System.setProperty("solr.autoCommit.maxTime", "600000");
    System.setProperty("solr.autoSoftCommit.maxTime", "-1");

    LOG.info("Loading the container...");
    CoreContainer container = new CoreContainer();
    container.load();
    for (String s : container.getAllCoreNames()) {
      LOG.warn("Got core name: " + s);
    }
    String coreName = "";
    if (container.getCoreNames().size() > 0) {
      coreName = container.getCoreNames().iterator().next();
    }

    LOG.error("Now firing up the server...");
    solrServer = new EmbeddedSolrServer(container, coreName);
    LOG.error("Server started successfully!");
  }

  @Override
  public void reduce(IntWritable key, Iterator<WritableSolrRecord> values,
      OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    WctEnricher wct;
    WritableSolrRecord wsr;
    SolrRecord solr;

    // Get the slice number, but counting from 1 instead of 0:
    int slice = key.get() + 1;

    // For indexing into HDFS, set up a new server per key:
    initEmbeddedServer(slice);

    // Go through the documents for this shard:
    long noValues = 0;
    while (values.hasNext()) {
      wsr = values.next();
      solr = wsr.getSolrRecord();
      noValues++;

      // Add additional metadata for WCT Instances.
      if (solr.containsKey(WctFields.WCT_INSTANCE_ID)) {
        wct = new WctEnricher(key.toString());
        wct.addWctMetadata(solr);
      }

      docs.add(solr.getSolrDocument());
      // Have we exceeded the batchSize?
      checkSubmission(docs, batchSize, reporter);

      // Occasionally update application-level status:
      if ((noValues % 1000) == 0) {
        reporter.setStatus(SHARD_PREFIX + slice + ": processed " + noValues + ", dropped "
            + reporter.getCounter(MyCounters.NUM_DROPPED_RECORDS).getValue());
      }
      if (this.exportXml
          && solr.getSolrDocument().getFieldValue(SolrFields.SOLR_URL_TYPE) != null
          && solr.getSolrDocument().getFieldValue(SolrFields.SOLR_URL_TYPE)
              .equals(SolrFields.SOLR_URL_TYPE_SLASHPAGE)) {
        output.collect(new Text(""),
            new Text(MetadataBuilder.SolrDocumentToElement(solr.getSolrDocument())));
      }
    }

    try {
      /**
       * If we have at least one document unsubmitted, make sure we submit it.
       */
      checkSubmission(docs, 1, reporter);

      // If we are indexing to HDFS, shut the shard down:
      // Commit, and block until the changes have been flushed.
      solrServer.commit(true, false);
      // And shut it down.
      solrServer.shutdown();
    } catch (Exception e) {
      LOG.error("ERROR on commit: " + e);
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
  }

  /**
   * Checks whether a List of docs has exceeded a given limit and if so, submits them.
   * 
   * @param docs
   * @param limit
   * @param reporter
   */
  private void checkSubmission(List<SolrInputDocument> docs, int limit, Reporter reporter) {
    if (docs.size() > 0 && docs.size() >= limit) {
      try {
        // Inform that there is progress (still-alive):
        reporter.progress();
        // Add the documents:
        UpdateResponse response = solrServer.add(docs);
        LOG.info("Submitted " + docs.size() + " docs [" + response.getStatus() + "]");
        // Update document counter:
        reporter.incrCounter(MyCounters.NUM_RECORDS, docs.size());
        // All good:
        docs.clear();
        numberOfSequentialFails = 0;
      } catch (Exception e) {
        // Count up repeated fails:
        numberOfSequentialFails++;

        // If there have been a lot of fails, drop the records
        // (we have seen some "Invalid UTF-8 character 0xfffe at char"
        // so this avoids bad data blocking job completion)
        if (this.numberOfSequentialFails >= 3) {
          LOG.error("Submission has repeatedly failed - assuming bad data and dropping these "
              + docs.size() + " records.");
          reporter.incrCounter(MyCounters.NUM_DROPPED_RECORDS, docs.size());
          docs.clear();
        }

        // SOLR-5719 possibly hitting us here;
        // CloudSolrServer.RouteException
        LOG.error("Sleeping for " + SUBMISSION_PAUSE_MINS + " minute(s): " + e.getMessage(), e);
        // Also add a report for this condition:
        reporter.incrCounter(MyCounters.NUM_ERRORS, 1);
        try {
          Thread.sleep(1000 * 60 * SUBMISSION_PAUSE_MINS);
        } catch (InterruptedException ex) {
          LOG.warn("Sleep between Solr submissions was interrupted!");
        }
      }
    }
  }
}
