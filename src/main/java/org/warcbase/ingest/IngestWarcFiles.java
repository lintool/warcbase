package org.warcbase.ingest;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.warcbase.Util;
import org.warcbase.WarcHTMLResponseRecord;
import org.warcbase.WarcRecord;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

public class IngestWarcFiles {
  private static final Logger LOG = Logger.getLogger(IngestWarcFiles.class);
  private static final int MAX_SIZE = 1024 * 1024;
  private static final Set<String> SKIP = ImmutableSet.of("mp3", "mov", "wmv", "mp4", "MP4");

  public static final String[] FAMILIES = {"content"};

  private final HTable table;
  private final HBaseAdmin admin;

  public IngestWarcFiles(String name, boolean create) throws Exception {
    Configuration hbaseConfig = HBaseConfiguration.create();
    admin = new HBaseAdmin(hbaseConfig);;
    
    if (admin.tableExists(name) && !create) {
      LOG.info(String.format("Table '%s' exists: doing nothing.", name));
    } else {
      if (admin.tableExists(name)) {
        LOG.info(String.format("Table '%s' exists: dropping table and recreating.", name));
        LOG.info(String.format("Disabling table '%s'", name));
        admin.disableTable(name);
        LOG.info(String.format("Droppping table '%s'", name));
        admin.deleteTable(name);
      }
      
      HTableDescriptor tableDesc = new HTableDescriptor(name);
      for (int i = 0; i < FAMILIES.length; i++) {
        tableDesc.addFamily(new HColumnDescriptor(FAMILIES[i]));
      }
      admin.createTable(tableDesc);
      LOG.info(String.format("Successfully created table '%s'", name));
    }

    table = new HTable(hbaseConfig, name);
    Field maxKeyValueSizeField = HTable.class.getDeclaredField("maxKeyValueSize");
    maxKeyValueSizeField.setAccessible(true);
    maxKeyValueSizeField.set(table, MAX_SIZE);
    
    LOG.info("Setting maxKeyValueSize to " + maxKeyValueSizeField.get(table));
    admin.close();
  }

  public void ingestFolder(File inputWarcFolder, int i) {
    long startTime = System.currentTimeMillis();
    int cnt = 0;
    int skipped = 0;
    for (; i < inputWarcFolder.listFiles().length; i++) {
      File inputWarcFile = inputWarcFolder.listFiles()[i];
      if (inputWarcFile.getName().charAt(0) == '.')
        continue;
      GZIPInputStream gzInputStream = null;
      try {
        LOG.info("Processing File: " + i + " = " + inputWarcFile.getName());
        gzInputStream = new GZIPInputStream(new FileInputStream(inputWarcFile));
      } catch (Exception e) {
        e.printStackTrace();
      }
      // cast to a data input stream
      DataInputStream inStream = new DataInputStream(gzInputStream);

      // iterate through our stream
      WarcRecord record;
      Map<String, String> parse = null;
      String thisTargetURI = null;
      try {
        while ((record = WarcRecord.readNextWarcRecord(inStream)) != null) {
          // see if it's a response record
          if (record.getHeaderRecordType().equals("response")) {
            // it is - create a WarcHTML record
            WarcHTMLResponseRecord htmlRecord = new WarcHTMLResponseRecord(record);
            // get our TREC ID and target URI
            thisTargetURI = htmlRecord.getTargetURI();
            if (SKIP.contains(Util.getUriExtension(thisTargetURI))) {
              skipped++;
            }

            String content = record.getContentUTF8();
            content = record.toString();
            parse = getHeaders(content);
            String key = Util.reverseHostname(thisTargetURI);
            if (key == null) {
              continue;
            }
            byte[] data = record.getByteContent();
            if ( data.length > MAX_SIZE) {
              LOG.info("Skipping " + key + " with " + data.length + " byte record");
              skipped++;
            } else {
              addRecord(key, parse.get("WARC-Date"), record.getByteContent());
              cnt++;
            }

            if (cnt % 1000 == 0) {
              LOG.info(cnt + " records ingested");
            }
          }
        }
        inStream.close();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    long totalTime = System.currentTimeMillis() - startTime;
    LOG.info("Total " + cnt + " records inserted, " + skipped + " records skipped");
    LOG.info("Total time: " + totalTime + "ms");
    LOG.info("Ingest rate: " + cnt / (totalTime/1000) + " records per second.");
  }

  private Map<String, String> getHeaders(String doc) {
    Map<String, String> hdr = Maps.newHashMapWithExpectedSize(20);
    try {
      BufferedReader in = new BufferedReader(new StringReader(doc));
      int nl = 0;
      String line = null;
      while ((line = in.readLine()) != null) {
        if (line.length() == 0)
          nl++;
        if (nl == 2)
          break;
        int i = line.indexOf(':');
        if (i == -1)
          continue;
        try {
          hdr.put(line.substring(0, i), line.substring(i + 2));
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      StringBuilder buf = new StringBuilder();
      while ((line = in.readLine()) != null) {
        buf.append(line).append('\n');
      }
      hdr.put("document", buf.toString());
    } catch (IOException e) {
      e.printStackTrace();
    }
    return hdr;
  }

  private void addRecord(String key, String date, byte[] data) {
    try {
      Put put = new Put(Bytes.toBytes(key));
      put.add(Bytes.toBytes(FAMILIES[0]), Bytes.toBytes(date), data);
      table.put(put);
    } catch (IOException e) {
      LOG.error("Couldn't insert key: " + key);
      LOG.error("File Size: " + data.length);
      e.printStackTrace();
    }
  }

  private static final String CREATE_OPTION = "create";
  private static final String APPEND_OPTION = "append";
  private static final String NAME_OPTION = "name";
  private static final String DIR_OPTION = "dir";
  private static final String START_OPTION = "start";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("name").hasArg()
        .withDescription("name of the archive").create(NAME_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("WARC files location").create(DIR_OPTION));
    options.addOption(OptionBuilder.withArgName("n").hasArg()
        .withDescription("Start from the n-th WARC file").create(START_OPTION));

    options.addOption("create", false, "create new table");
    options.addOption("append", false, "append to existing table");

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(DIR_OPTION) || !cmdline.hasOption(NAME_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(IngestWarcFiles.class.getCanonicalName(), options);
      System.exit(-1);
    }

    if (!cmdline.hasOption(CREATE_OPTION) && !cmdline.hasOption(APPEND_OPTION)) {
      System.err.println(String.format("Must specify either -%s or -%s", CREATE_OPTION, APPEND_OPTION));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(IngestWarcFiles.class.getCanonicalName(), options);
      System.exit(-1);
    }

    String path = cmdline.getOptionValue(DIR_OPTION);
    File inputWarcFolder = new File(path);

    int i = 0;
    if (cmdline.hasOption(START_OPTION)) {
      i = Integer.parseInt(cmdline.getOptionValue(START_OPTION));
    }

    String name = cmdline.getOptionValue(NAME_OPTION);
    boolean create = cmdline.hasOption(CREATE_OPTION);
    IngestWarcFiles load = new IngestWarcFiles(name, create);

    load.ingestFolder(inputWarcFolder, i);
  }
}
