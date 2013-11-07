package org.warcbase.ingest;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import org.jruby.RubyProcess.Sys;
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
import org.warcbase.data.Util;

public class SearchForUri {
  private static final String DIR_OPTION = "dir";
  private static final String URI_OPTION = "uri";
  private static final Logger LOG = Logger.getLogger(SearchForUri.class);
  
  private void searchArcFile(File inputArcFile, String uri) throws IOException{
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
      type = record.getContentTypeStr();
      content = IOUtils.toByteArray(record.getPayloadContent());
      key = Util.reverseHostname(url);
      if(uri.equals(url)){
        System.out.println("-----------------------------------");
        System.out.println("Found at " + inputArcFile.getName());
        System.out.println(date);
        System.out.println(type);
        System.out.println(content.length);
        System.out.println(key);
      }
    }
  }
  
  private void searchWarcFile(File inputWarcFile, String uri) throws FileNotFoundException, IOException{
    WarcReader warcReader = null;
    org.jwat.warc.WarcRecord warcRecord = null;
    String url = null;
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
        url = warcRecord.header.warcTargetUriStr;
        key = Util.reverseHostname(url);
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
          if(uri.equals(url)){
            System.out.println("-----------------------------------");
            System.out.println("Found at " + inputWarcFile.getName());
            System.out.println(date);
            System.out.println(type);
            System.out.println(content.length);
            System.out.println(key);
          }
          
      }
    }
   }
  }
  
  private void searchFolder(File inputFolder, String uri) throws IOException{
    long startTime = System.currentTimeMillis();
    RandomAccessFile raf = null;
    RandomAccessFileInputStream rafin = null;
    ByteCountingPushBackInputStream pbin = null;
    GZIPInputStream gzInputStream = null;
    
    for (int i = 0; i < inputFolder.listFiles().length; i++) {
      File inputFile = inputFolder.listFiles()[i];
      LOG.info("processing file " + i + ": " + inputFile.getName());
      raf = new RandomAccessFile( inputFile, "r" );
      rafin = new RandomAccessFileInputStream( raf );
      pbin = new ByteCountingPushBackInputStream( new BufferedInputStream( rafin, 8192 ), 32 );
      if ( GzipReader.isGzipped( pbin ) ){
        gzInputStream = new GZIPInputStream(new FileInputStream(inputFile));
        ByteCountingPushBackInputStream in = new ByteCountingPushBackInputStream(gzInputStream, 32);
        if(ArcReaderFactory.isArcFile( in )){
          searchArcFile(inputFile, uri);
        }
        else if ( WarcReaderFactory.isWarcFile( in ) ) {
          searchWarcFile(inputFile, uri);
        }
      }
    }
    long totalTime = System.currentTimeMillis() - startTime;
    LOG.info("Total time: " + totalTime + "ms");
  }
  
  
  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("WARC files location").create(DIR_OPTION));
    options.addOption(OptionBuilder.withArgName("uri").hasArg()
        .withDescription("search for URI in warc files").create(URI_OPTION));
    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }
    
    if (!cmdline.hasOption(DIR_OPTION) || !cmdline.hasOption(URI_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(IngestFiles.class.getCanonicalName(), options);
      System.exit(-1);
    }
    
    String path = cmdline.getOptionValue(DIR_OPTION);
    String uri = cmdline.getOptionValue(URI_OPTION);
    File inputFolder = new File(path);
    
    SearchForUri search = new SearchForUri();
    search.searchFolder(inputFolder, uri);
  }
}
