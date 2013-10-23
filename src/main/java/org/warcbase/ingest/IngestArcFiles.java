package org.warcbase.ingest;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.jwat.arc.ArcReaderCompressed;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcRecord;
import org.jwat.arc.ArcRecordBase;
import org.jwat.arc.ArcVersionHeader;
import org.jwat.common.ByteCountingPushBackInputStream;
import org.jwat.common.HeaderLine;
import org.jwat.common.HttpHeader;
import org.jwat.common.Payload;
import org.jwat.gzip.GzipEntry;
import org.jwat.gzip.GzipReader;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.warcbase.data.ArcParser;
import org.warcbase.data.Util;

import com.google.common.collect.ImmutableSet;

public class IngestArcFiles {
  private static final String CREATE_OPTION = "create";
  private static final String APPEND_OPTION = "append";
  private static final String NAME_OPTION = "name";
  private static final String DIR_OPTION = "dir";
  private static final String START_OPTION = "start";
  
  private static final Logger LOG = Logger.getLogger(IngestArcFiles.class);
  private static final int MAX_SIZE = 1024 * 1024;
  private static final Set<String> SKIP = ImmutableSet.of("mp3", "mov", "wmv", "mp4", "MP4");
  
  private final int PUSHBACK_BUFFER_SIZE = 16;

  public static final String[] FAMILIES = {"content", "type"};

  private final HTable table;
  private final HBaseAdmin admin;

  public IngestArcFiles(String name, boolean create) throws IOException, SecurityException, NoSuchFieldException, IllegalArgumentException, IllegalAccessException {
    // TODO Auto-generated constructor stub
    admin = null;
    table = null;
    if(true)
      return;
    
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
  

  public static void printRecord(ArcRecordBase record) {
    System.out.println("--------------");
    System.out.println("       TypeIdx: " + record.recordType);
    System.out.println("          Type: " + record.getContentTypeStr());
    System.out.println("      Filename: " + record.getFileName());
    System.out.println("     Record-ID: " + record.getUrlStr());
    System.out.println("          Date: " + record.getArchiveDate().getTime());
    System.out.println("          Date2: " + record.getArchiveDateStr());
    System.out.println("Content-LengthStr: " + record.getArchiveLengthStr());
    System.out.println("Content-Length: " + record.getArchiveLength());
    System.out.println("  Content-Type: " + record.getContentType());
    System.out.println("   Data: " + record.getVersion());
    System.out.println(record.getResultCodeStr());
    System.out.println(record.toString());
    byte[] b = null;
    //ArcRecord record2 = (ArcRecord) record;
    try {
      b = IOUtils.toByteArray(record.getPayloadContent());
      if(record.getContentTypeStr().equals("text/html"))
        System.out.println(new String(b, "UTF8"));
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
}
  
  /*private void ingestFolder(File inputArcFolder, int i) {
    // TODO Auto-generated method stub
    long startTime = System.currentTimeMillis();
    int cnt = 0;
    int skipped = 0;
    InputStream in = null;
    GzipReader reader;
    GzipEntry currentEntry = null;
    ByteCountingPushBackInputStream pbin = null;
    byte[] arcRecord = null;
    
    for (; i < inputArcFolder.listFiles().length; i++) {
      File inputArcFile = inputArcFolder.listFiles()[i];
      if (inputArcFile.getName().charAt(0) == '.')
        continue;
      try {
        in = new FileInputStream( inputArcFile );
      } catch (FileNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      pbin =
          new ByteCountingPushBackInputStream(
                  new BufferedInputStream(in),
                                          PUSHBACK_BUFFER_SIZE);
      reader = new GzipReader(pbin);
      
      try {
        while((currentEntry = reader.getNextEntry()) != null){
          arcRecord = IOUtils.toByteArray(currentEntry.getInputStream());
          ArcParser arcParser = new ArcParser(arcRecord);
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      
    }
    
  }*/
  
  /*private void ingestFolder(File inputArcFolder, int i) {
    System.out.println("salam");
    File inputArcFile = inputArcFolder;
    InputStream in = null;
    try {
      in = new FileInputStream( inputArcFile );
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    ArcReader reader = null;
    try {
      reader = ArcReaderFactory.getReader(in);
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    ArcRecordBase record;
    try {
      while((record = reader.getNextRecord()) != null){
        printRecord(record);
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }*/
  
  private void addRecord(String key, String date, byte[] data, String type) {
    try {
      Put put = new Put(Bytes.toBytes(key));
      put.add(Bytes.toBytes(FAMILIES[0]), Bytes.toBytes(date), data);
      if(type == null || Bytes.toBytes(type) == null){
        System.out.println(key);
        System.out.println(date);
        System.out.println(type);
      }
      put.add(Bytes.toBytes(FAMILIES[1]), Bytes.toBytes(date), Bytes.toBytes(type));
      table.put(put);
    } catch (IOException e) {
      LOG.error("Couldn't insert key: " + key);
      LOG.error("File Size: " + data.length);
      e.printStackTrace();
    }
  }
  
  private void ingestFolder(File inputArcFolder, int i) {
    long startTime = System.currentTimeMillis();
    int cnt = 0;
    int skipped = 0;
    InputStream in = null;
    ArcReader reader = null;
    ArcRecordBase record = null;
    String url = null;
    String date = null;
    byte[] content = null;
    String type = null;
    String key = null;
    
    for (; i < inputArcFolder.listFiles().length; i++) {
      if(cnt % 10000 == 0 && cnt > 0){
        LOG.info("Ingested " + cnt + "records to Hbase.");
      }
      File inputArcFile = inputArcFolder.listFiles()[i];
      try {
        in = new FileInputStream( inputArcFile );
      } catch (FileNotFoundException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      try {
        reader = ArcReaderFactory.getReader(in);
      } catch (IOException e1) {
        // TODO Auto-generated catch block
        e1.printStackTrace();
      }
      try {
        while((record = reader.getNextRecord()) != null){
          //printRecord(record);
          url = record.getUrlStr();
          date = record.getArchiveDateStr();
          content = IOUtils.toByteArray(record.getPayloadContent());
          key = Util.reverseHostname(url);
          type = record.getContentTypeStr();
          
          if (key == null) {
            continue;
          }
          if(record.getArchiveLength() > MAX_SIZE){
            LOG.info("Skipping " + key + " with " + record.getArchiveLength() + " byte record");
            skipped++;
          }
          else{
            //if(type.equals("text/html"))
              //System.out.println(new String(content, "UTF8"));
            if(type == null)
               type = "text/plain";
            addRecord(key, date, content, type);
            cnt++;
          }
        }
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }
  

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws SecurityException, IllegalArgumentException, IOException, NoSuchFieldException, IllegalAccessException {
    //IngestArcFiles load = new IngestArcFiles("", false);
    //load.ingestFolder(new File("/scratch0/webarchive/congress108/arc.sample/CONGRESS01-20040124030547-15.arc.gz"), 0);
    
    //if(true)
      //return;
    
    
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("name").hasArg()
        .withDescription("name of the archive").create(NAME_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("ARC files location").create(DIR_OPTION));
    options.addOption(OptionBuilder.withArgName("n").hasArg()
        .withDescription("Start from the n-th ARC file").create(START_OPTION));

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

    /*if (!cmdline.hasOption(CREATE_OPTION) && !cmdline.hasOption(APPEND_OPTION)) {
      System.err.println(String.format("Must specify either -%s or -%s", CREATE_OPTION, APPEND_OPTION));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(IngestWarcFiles.class.getCanonicalName(), options);
      System.exit(-1);
    }*/

    String path = cmdline.getOptionValue(DIR_OPTION);
    File inputArcFolder = new File(path);
    
    int i = 0;
    if (cmdline.hasOption(START_OPTION)) {
      i = Integer.parseInt(cmdline.getOptionValue(START_OPTION));
    }
    String name = cmdline.getOptionValue(NAME_OPTION);
    boolean create = cmdline.hasOption(CREATE_OPTION);
    
    IngestArcFiles load = new IngestArcFiles(name, create);
    load.ingestFolder(inputArcFolder, i);
  }

}
