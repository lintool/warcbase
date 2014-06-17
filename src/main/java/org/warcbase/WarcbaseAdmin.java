package org.warcbase;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class WarcbaseAdmin {
  private static final Logger LOG = Logger.getLogger(WarcbaseAdmin.class);

  private static final String METATABLE = "warcbase.meta";

  private static final String INITIALIZE_OPTION = "initialize";
  private static final String FORCE_OPTION = "force";

  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(new Option(INITIALIZE_OPTION, "initialize metadata table"));
    options.addOption(new Option(FORCE_OPTION, "force"));

    Logger.getLogger(org.apache.zookeeper.ZooKeeper.class).setLevel(Level.WARN);
    Logger.getLogger(org.apache.zookeeper.ClientCnxn.class).setLevel(Level.WARN);
    Logger.getLogger(org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper.class).setLevel(Level.WARN);
    Logger.getLogger(org.apache.hadoop.hbase.client.HConnectionManager.class).setLevel(Level.WARN);

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    Configuration hbaseConfig = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(hbaseConfig);

    LOG.info("Successfully created connection to HBase.");
    if (cmdline.hasOption(INITIALIZE_OPTION)) {
      if (!admin.tableExists(METATABLE) || cmdline.hasOption(FORCE_OPTION)) {
        if (admin.tableExists(METATABLE)) {
          LOG.info(METATABLE + " exists already. Dropping.");
          admin.disableTable(METATABLE);
          admin.deleteTable(METATABLE);
        }

        HTableDescriptor tableDesc = new HTableDescriptor(METATABLE);
        HColumnDescriptor hColumnDesc = new HColumnDescriptor("m");
        tableDesc.addFamily(hColumnDesc);
        admin.createTable(tableDesc);
        LOG.info("Sucessfully created " + METATABLE);
      } else {
        LOG.info(METATABLE + " exists already. Doing nothing.");
        LOG.info("To destory existing " + METATABLE + " and reinitialize, use -" + FORCE_OPTION);
      }
    }

    admin.close();
  }
}
