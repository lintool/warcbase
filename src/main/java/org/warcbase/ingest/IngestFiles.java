package org.warcbase.ingest;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.zip.GZIPInputStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecordBase;
import org.jwat.common.ByteCountingPushBackInputStream;
import org.jwat.common.HttpHeader;
import org.jwat.common.Payload;
import org.jwat.common.RandomAccessFileInputStream;
import org.jwat.common.UriProfile;
import org.jwat.gzip.GzipReader;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.warcbase.data.HbaseManager;
import org.warcbase.data.Util;

public class IngestFiles {
  private static final String CREATE_OPTION = "create";
  private static final String APPEND_OPTION = "append";
  private static final String NAME_OPTION = "name";
  private static final String DIR_OPTION = "dir";
  private static final String START_OPTION = "start";
  
  private static final Logger LOG = Logger.getLogger(IngestFiles.class);
  public static final int MAX_CONTENT_SIZE = 1024 * 1024;
  private static int cnt = 0;
  private static int skipped = 0;

  private final HbaseManager hbaseManager;

  public IngestFiles(String name, boolean create) throws Exception {
    hbaseManager = new HbaseManager(name, create);
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
         if(hbaseManager.addRecord(key, date, content, type)){
            cnt++;
          }
         else{
           skipped++;
        }
      }
    }
  }
  
  private void ingestWarcFile(File inputWarcFile) throws IOException {
    WarcReader warcReader = null;
    org.jwat.warc.WarcRecord warcRecord = null;
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
              if(hbaseManager.addRecord(key, date, content, type)){
                cnt++;
              }
              else
                skipped++;
            }
        }
      }
    }
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
        
    for (; i < inputFolder.listFiles().length; i++) {
      File inputFile = inputFolder.listFiles()[i];
      LOG.info("processing file " + i + ": " + inputFile.getName());
      //System.out.println("processing file " + i + ": " + inputFile.getName());
      raf = new RandomAccessFile( inputFile, "r" );
      rafin = new RandomAccessFileInputStream( raf );
      pbin = new ByteCountingPushBackInputStream( new BufferedInputStream( rafin, 8192 ), 32 );
      if ( GzipReader.isGzipped( pbin ) ){
        gzInputStream = new GZIPInputStream(new FileInputStream(inputFile));
        ByteCountingPushBackInputStream in = new ByteCountingPushBackInputStream(gzInputStream, 32);
        if(ArcReaderFactory.isArcFile( in )){
          ingestArcFile(inputFile);
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
