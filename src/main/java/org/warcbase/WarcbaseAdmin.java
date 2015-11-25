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

package org.warcbase;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class WarcbaseAdmin {
  private static final Logger LOG = Logger.getLogger(WarcbaseAdmin.class);

  private static final String METATABLE = "warcbase.meta";

  private static final String INITIALIZE_OPTION = "initialize";
  private static final String FORCE_OPTION = "force";
  private static final String HELP_OPTION = "help";
  private static final String ADD_OPTION = "addCollection";
  private static final String DELETE_OPTION = "deleteCollection";
  private static final String DUMP_OPTION = "dump";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(new Option(INITIALIZE_OPTION, "initialize metadata table"));
    options.addOption(new Option(FORCE_OPTION, "force initialization of metadata table"));
    options.addOption(new Option(HELP_OPTION, "prints help message"));
    options.addOption(new Option(DUMP_OPTION, "dumps metadata table"));

    options.addOption(OptionBuilder.withArgName("name").hasArg()
        .withDescription("add a collection").create(ADD_OPTION));
    options.addOption(OptionBuilder.withArgName("name").hasArg()
        .withDescription("remove a collection").create(DELETE_OPTION));

    // ZooKeeper is fairly noisy in logging. Normally, not a big deal, but in this case
    // gets in the way.
    Logger.getLogger(org.apache.zookeeper.ZooKeeper.class).setLevel(Level.WARN);
    Logger.getLogger(org.apache.zookeeper.ClientCnxn.class).setLevel(Level.WARN);
    Logger.getLogger(org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper.class).setLevel(Level.WARN);
    Logger.getLogger(org.apache.hadoop.hbase.client.HConnectionManager.class).setLevel(Level.WARN);

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(WarcbaseAdmin.class.getName(), options);
      System.exit(-1);
    }

    if (cmdline.hasOption(HELP_OPTION) || cmdline.getOptions().length == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(WarcbaseAdmin.class.getName(), options);
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
    } else if (cmdline.hasOption(ADD_OPTION)) {
      String name = cmdline.getOptionValue(ADD_OPTION);
      HTable table = new HTable(hbaseConfig, METATABLE);

      Get get = new Get(Bytes.toBytes(name));
      if (table.get(get).isEmpty()) {
        Put put = new Put(Bytes.toBytes(name));
        put.add(Bytes.toBytes("m"), Bytes.toBytes(name), Bytes.toBytes(System.currentTimeMillis() + ""));
        table.put(put);
        LOG.info("Adding collection '" + name + "'");
      } else {
        LOG.info("Error, collection '" + name + "' already exists!");
      }
      table.close();
    } else if (cmdline.hasOption(DELETE_OPTION)) {
      String name = cmdline.getOptionValue(DELETE_OPTION);
      HTable table = new HTable(hbaseConfig, METATABLE);

      Get get = new Get(Bytes.toBytes(name));
      if (table.get(get).isEmpty()) {
        LOG.info("Error, collection '" + name + "' doesn't exist!");
      } else {
        Delete delete = new Delete(Bytes.toBytes(name));
        table.delete(delete);
        LOG.info("Deleted collection '" + name + "'");
      }
      table.close();
    } else if (cmdline.hasOption(DUMP_OPTION)) {
      HTable table = new HTable(hbaseConfig, METATABLE);
      ResultScanner scanner = table.getScanner(Bytes.toBytes("m"));
      Result result;
      while ( (result = scanner.next()) != null) {
        LOG.info("---> collection: " + new String(result.getRow()));
        for ( KeyValue kv : result.list()) {
          LOG.info(new String(kv.getFamily()) + ":" + new String(kv.getQualifier()) +
              ", length=" + kv.getValue().length);
        }
      }
      table.close();
    }

    admin.close();
  }
}
