package org.warcbase.ingest;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecordBase;
import org.jwat.common.ByteCountingPushBackInputStream;
import org.jwat.common.HttpHeader;
import org.jwat.common.Payload;
import org.jwat.common.RandomAccessFileInputStream;
import org.jwat.common.UriProfile;
import org.jwat.gzip.GzipEntry;
import org.jwat.gzip.GzipReader;
import org.jwat.tools.core.FileIdent;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.warcbase.data.Util;

import com.google.common.collect.ImmutableSet;

public class IngestFiles {
  private static final String CREATE_OPTION = "create";
  private static final String APPEND_OPTION = "append";
  private static final String NAME_OPTION = "name";
  private static final String DIR_OPTION = "dir";
  private static final String START_OPTION = "start";
  
  private final int PUSHBACK_BUFFER_SIZE = 16;
  private static final Logger LOG = Logger.getLogger(IngestFiles.class);
  protected static final int MAX_CONTENT_SIZE = 1024 * 1024;
  private static final int MAX_KEY_VALUE_SIZE = MAX_CONTENT_SIZE + 200;
  private static final Set<String> SKIP = ImmutableSet.of("mp3", "mov", "wmv", "mp4", "MP4");
  private static int cnt = 0;
  private static int skipped = 0;

  public static final String[] FAMILIES = {"content", "type"};

  private final HTable table;
  private final HBaseAdmin admin;

  public IngestFiles(String name, boolean create) throws Exception {
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
    maxKeyValueSizeField.set(table, MAX_KEY_VALUE_SIZE);
    
    LOG.info("Setting maxKeyValueSize to " + maxKeyValueSizeField.get(table));
    admin.close();

  }
  
  protected Boolean addRecord(String key, String date, byte[] data, String type) {
    System.out.println(key);
    if(true) return true;
    try {
      Put put = new Put(Bytes.toBytes(key));
      put.add(Bytes.toBytes(FAMILIES[0]), Bytes.toBytes(date), data);
      put.add(Bytes.toBytes(FAMILIES[1]), Bytes.toBytes(date), Bytes.toBytes(type));
      table.put(put);
      return true;
    } catch (IOException e) {
      LOG.error("IOException: Couldn't insert key: " + key);
      LOG.error("File Size: " + data.length);
      e.printStackTrace();
      return false;
    } catch (IllegalArgumentException e) {
      // TODO: handle exception
      LOG.error("IllegalArgumentException: Couldn't insert key: " + key);
      LOG.error("File Size: " + data.length);
      return false;
    }
  }

  
  //protected RandomAccessFile raf = null;
  
  public static long parse(File file) {
    UriProfile uriProfile = UriProfile.RFC3986_ABS_16BIT_LAX;
    boolean bBlockDigestEnabled = true;
    boolean bPayloadDigestEnabled = true;
    int recordHeaderMaxSize = 8192;
    int payloadHeaderMaxSize = 32768;
    
    RandomAccessFile raf = null;
    RandomAccessFileInputStream rafin = null;
    ByteCountingPushBackInputStream pbin = null;
    GzipReader gzipReader = null;
    ArcReader arcReader = null;
    WarcReader warcReader = null;
    WarcReader warcReader2 = null;
    GzipEntry gzipEntry = null;
    ArcRecordBase arcRecord = null;
    WarcRecord warcRecord = null;
    byte[] buffer = new byte[ 8192 ];
    String uri = null;
    String key = null;
    try {
      raf = new RandomAccessFile( file, "r" );
      rafin = new RandomAccessFileInputStream( raf );
      pbin = new ByteCountingPushBackInputStream( new BufferedInputStream( rafin, 8192 ), 32 );
      if ( GzipReader.isGzipped( pbin ) ) {
        gzipReader = new GzipReader( pbin );
        ByteCountingPushBackInputStream in;
        ByteCountingPushBackInputStream in2;
        int gzipEntries = 0;
        while ( (gzipEntry = gzipReader.getNextEntry()) != null ) {
          in = new ByteCountingPushBackInputStream( new BufferedInputStream( gzipEntry.getInputStream(), 8192 ), 32 );
          //in2 = new ByteCountingPushBackInputStream(new FileInputStream(file), 32);
          GZIPInputStream gzInputStream = null;
          gzInputStream = new GZIPInputStream(new FileInputStream(file));
          warcReader2 = WarcReaderFactory.getReaderCompressed();
          in2 = new ByteCountingPushBackInputStream(gzInputStream, 32);
          ++gzipEntries;
          //System.out.println(gzipEntries + " - " + gzipEntry.getStartOffset() + " (0x" + (Long.toHexString(gzipEntry.getStartOffset())) + ")");
          if ( gzipEntries == 1 ) {
            if ( ArcReaderFactory.isArcFile( in ) ) {
              arcReader = ArcReaderFactory.getReaderUncompressed();
              arcReader.setUriProfile(uriProfile);
              arcReader.setBlockDigestEnabled( bBlockDigestEnabled );
              arcReader.setPayloadDigestEnabled( bPayloadDigestEnabled );
              arcReader.setRecordHeaderMaxSize( recordHeaderMaxSize );
              arcReader.setPayloadHeaderMaxSize( payloadHeaderMaxSize );
            }
            else if ( WarcReaderFactory.isWarcFile( in ) ) {
              warcReader = WarcReaderFactory.getReaderUncompressed();
              warcReader.setWarcTargetUriProfile(uriProfile);
              warcReader.setBlockDigestEnabled( bBlockDigestEnabled );
              warcReader.setPayloadDigestEnabled( bPayloadDigestEnabled );
              warcReader.setRecordHeaderMaxSize( recordHeaderMaxSize );
              warcReader.setPayloadHeaderMaxSize( payloadHeaderMaxSize );
            }
            else {
            }
          }
          if ( arcReader != null ) {
            while ( (arcRecord = arcReader.getNextRecordFrom( in, gzipEntry.getStartOffset() )) != null ) {
            }
          }
          else if ( warcReader != null ) {
            while ( (warcRecord = warcReader.getNextRecordFrom( in2, 0 ) ) != null ) {
            //while ( (warcRecord = warcReader.getNextRecordFrom( in, gzipEntry.getStartOffset() ) ) != null ) {
            //while ( (warcRecord = warcReader2.getNextRecordFrom(in2, 0) ) != null ) {
              uri = warcRecord.header.warcTargetUriStr;
              System.out.println(uri);
              key = Util.reverseHostname(uri);
              if(key != null && key.equals("gov.house.www/")){
                System.out.println("##################################################################");
                System.out.println("at " + file.getName());
                System.out.println("Not added yet");
              }
            }
          }
          else {
            while ( in.read(buffer) != -1 ) {
            }
          }
          in.close();
          gzipEntry.close();
        }
      }
      else if ( ArcReaderFactory.isArcFile( pbin ) ) {
        arcReader = ArcReaderFactory.getReaderUncompressed( pbin );
        arcReader.setUriProfile(uriProfile);
        arcReader.setBlockDigestEnabled( bBlockDigestEnabled );
        arcReader.setPayloadDigestEnabled( bPayloadDigestEnabled );
        arcReader.setRecordHeaderMaxSize( recordHeaderMaxSize );
        arcReader.setPayloadHeaderMaxSize( payloadHeaderMaxSize );
        while ( (arcRecord = arcReader.getNextRecord()) != null ) {
        }
        arcReader.close();
      }
      else if ( WarcReaderFactory.isWarcFile( pbin ) ) {
        warcReader = WarcReaderFactory.getReaderUncompressed( pbin );
        warcReader.setWarcTargetUriProfile(uriProfile);
        warcReader.setBlockDigestEnabled( bBlockDigestEnabled );
        warcReader.setPayloadDigestEnabled( bPayloadDigestEnabled );
        warcReader.setRecordHeaderMaxSize( recordHeaderMaxSize );
        warcReader.setPayloadHeaderMaxSize( payloadHeaderMaxSize );
        System.err.println("Uncompressed.");
        while ( (warcRecord = warcReader.getNextRecord()) != null ) {
          uri = warcRecord.header.warcTargetUriStr;
          System.out.println(uri);
          key = Util.reverseHostname(uri);
          if(key != null && key.equals("gov.house.www/")){
            System.out.println("##################################################################");
            System.out.println("at " + file.getName());
            System.out.println("Added.");
          }
        }
        warcReader.close();
      }
      else {
      }
    }
    catch (Throwable t) {
      // TODO just use reader.getStartOffset?
      long startOffset = -1;
      Long length = null;
      if (arcRecord != null) {
        startOffset = arcRecord.getStartOffset();
        length = arcRecord.header.archiveLength;
      }
      if (warcRecord != null) {
        startOffset = warcRecord.getStartOffset();
        length = warcRecord.header.contentLength;
      }
      if (gzipEntry != null) {
        startOffset = gzipEntry.getStartOffset();
        // TODO correct entry size including header+trailer.
        length = gzipEntry.compressed_size;
      }
      if (length != null) {
        startOffset += length;
      }
    }
    finally {
      if ( arcReader != null ) {
        arcReader.close();
      }
      if ( warcReader != null ) {
        warcReader.close();
      }
      if (gzipReader != null) {
        try {
          gzipReader.close();
        }
        catch (IOException e) {
        }
      }
      if (pbin != null) {
        try {
          pbin.close();
        }
        catch (IOException e) {
        }
      }
      if (raf != null) {
        try {
          raf.close();
        }
        catch (IOException e) {
        }
      }
    }
    return pbin.getConsumed();
  }
  
  
  private void ingestArcFile(File inputArcFile) throws IOException {
    InputStream in = null;
    ArcReader reader = null;
    ArcRecordBase record = null;
    String url = null;
    String date = null;
    byte[] content = null;
    String type = null;
    String key = null;
    
    in = new FileInputStream( inputArcFile );
    reader = ArcReaderFactory.getReader(in);
    while((record = reader.getNextRecord()) != null){
      url = record.getUrlStr();
      date = record.getArchiveDateStr();
      content = IOUtils.toByteArray(record.getPayloadContent());
      key = Util.reverseHostname(url);
      type = record.getContentTypeStr();
      
      if(key != null && type == null){
        type = "text/plain";
      }
          
      if (key == null) {
        continue;
       }
       if(content.length > MAX_CONTENT_SIZE){
         skipped++;
       }
       else{
         if(cnt % 10000 == 0 && cnt > 0){
           LOG.info("Ingested " + cnt + "records to Hbase.");
         }
         if(addRecord(key, date, content, type)){
            cnt++;
          }
         else{
           skipped++;
        }
      }
    }
  }
  
  private void ingestWarcFile(File inputWarcFile) throws IOException {
    InputStream inFile = null;
    WarcReader reader = null;
    WarcReader warcReader = null;
    org.jwat.warc.WarcRecord warcRecord = null;
    org.jwat.warc.WarcRecord record = null;
    String uri = null;
    String date = null;
    String type = null;
    byte[] content = null;
    String key = null;
    
    UriProfile uriProfile = UriProfile.RFC3986_ABS_16BIT_LAX;
    boolean bBlockDigestEnabled = true;
    boolean bPayloadDigestEnabled = true;
    int recordHeaderMaxSize = 8192;
    int payloadHeaderMaxSize = 32768;

    
    GZIPInputStream gzInputStream = null;
    gzInputStream = new GZIPInputStream(new FileInputStream(inputWarcFile));
    ByteCountingPushBackInputStream pbin = new ByteCountingPushBackInputStream( new BufferedInputStream( gzInputStream, 8192 ), 32 );
    warcReader = WarcReaderFactory.getReaderUncompressed( pbin );
    if(warcReader == null)
       return;
    warcReader.setWarcTargetUriProfile(uriProfile);
    warcReader.setBlockDigestEnabled( bBlockDigestEnabled );
    warcReader.setPayloadDigestEnabled( bPayloadDigestEnabled );
    warcReader.setRecordHeaderMaxSize( recordHeaderMaxSize );
    warcReader.setPayloadHeaderMaxSize( payloadHeaderMaxSize );
    if ( warcReader != null ) {
      while ( (warcRecord = warcReader.getNextRecord()) != null ) {
        uri = warcRecord.header.warcTargetUriStr;
        key = Util.reverseHostname(uri);
        Payload payload = warcRecord.getPayload();
        HttpHeader httpHeader = null;
        InputStream payloadStream = null;
        if (payload != null) {
          httpHeader = warcRecord.getHttpHeader();
          if (httpHeader != null ) {
            payloadStream = httpHeader.getPayloadInputStream();
            type = httpHeader.contentType;
          } else {
            payloadStream = payload.getInputStreamComplete();
          }
        }
        date = warcRecord.header.warcDateStr;
        if (payloadStream != null) {
          content = IOUtils.toByteArray(payloadStream);
          if(key != null && type == null){//key.equals("gov.house.bernie/application/text_only/index.asp")){
            type = "text/plain";
          }
          if(key != null && warcRecord.getHeader("WARC-Type").value.toLowerCase().equals("response")){
            if(content.length > MAX_CONTENT_SIZE){
              skipped++;
            }
            else{
              if(cnt % 10000 == 0 && cnt > 0){
                LOG.info("Ingested " + cnt + "records to Hbase.");
              }
              if(addRecord(key, date, content, type)){
                cnt++;
              }
              else
                skipped++;
            }
        }
      }
    }
   }
   else{
     LOG.info("warcReader is null");
   }
         
  }
  
  private void ingestFolder(File inputFolder, int i) throws IOException {
    long startTime = System.currentTimeMillis();
    cnt = 0;
    skipped = 0;
    RandomAccessFile raf = null;
    RandomAccessFileInputStream rafin = null;
    ByteCountingPushBackInputStream pbin = null;
    GZIPInputStream gzInputStream = null;
    
    InputStream inFile = null;
    
    for (; i < inputFolder.listFiles().length; i++) {
      File inputFile = inputFolder.listFiles()[i];
      raf = new RandomAccessFile( inputFile, "r" );
      rafin = new RandomAccessFileInputStream( raf );
      pbin = new ByteCountingPushBackInputStream( new BufferedInputStream( rafin, 8192 ), 32 );
      if ( GzipReader.isGzipped( pbin ) ){
        gzInputStream = new GZIPInputStream(new FileInputStream(inputFile));
        ByteCountingPushBackInputStream in = new ByteCountingPushBackInputStream(gzInputStream, 32);
        if(ArcReaderFactory.isArcFile( in )){
          //ingestArcFile(inputFile);
        }
        else if ( WarcReaderFactory.isWarcFile( in ) ) {
          ingestWarcFile(inputFile);
        }
      }
        
    }
    
    long totalTime = System.currentTimeMillis() - startTime;
    LOG.info("Total " + cnt + " records inserted, " + skipped + " records skipped");
    LOG.info("Total time: " + totalTime + "ms");
    LOG.info("Ingest rate: " + cnt / (totalTime/1000) + " records per second.");
  }
  
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
      formatter.printHelp(IngestFiles.class.getCanonicalName(), options);
      System.exit(-1);
    }

    if (!cmdline.hasOption(CREATE_OPTION) && !cmdline.hasOption(APPEND_OPTION)) {
      System.err.println(String.format("Must specify either -%s or -%s", CREATE_OPTION, APPEND_OPTION));
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(IngestFiles.class.getCanonicalName(), options);
      System.exit(-1);
    }

    String path = cmdline.getOptionValue(DIR_OPTION);
    File inputFolder = new File(path);

    int i = 0;
    if (cmdline.hasOption(START_OPTION)) {
      i = Integer.parseInt(cmdline.getOptionValue(START_OPTION));
    }

    String name = cmdline.getOptionValue(NAME_OPTION);
    boolean create = cmdline.hasOption(CREATE_OPTION);
    IngestFiles load = new IngestFiles(name, create);
    load.ingestFolder(inputFolder, i);
  }
}
