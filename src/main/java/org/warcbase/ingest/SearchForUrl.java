package org.warcbase.ingest;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
import org.jwat.common.UriProfile;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.warcbase.data.UrlUtils;

public class SearchForUrl {
  private static final String DIR_OPTION = "dir";
  private static final String URI_OPTION = "uri";
  private static final Logger LOG = Logger.getLogger(SearchForUrl.class);
  private static final UriProfile uriProfile = UriProfile.RFC3986_ABS_16BIT_LAX;
  private static final boolean bBlockDigestEnabled = true;
  private static final boolean bPayloadDigestEnabled = true;
  private static final int recordHeaderMaxSize = 8192;
  private static final int payloadHeaderMaxSize = 32768;

  private void searchArcFile(File inputArcFile, String uri) throws IOException {
    ArcRecordBase record = null;
    String url = null;
    String date = null;
    byte[] content = null;
    String type = null;
    String key = null;

    InputStream in = new FileInputStream(inputArcFile);
    ArcReader reader = ArcReaderFactory.getReader(in);
    while ((record = reader.getNextRecord()) != null) {
      url = record.getUrlStr();
      date = record.getArchiveDateStr();
      type = record.getContentTypeStr();
      content = IOUtils.toByteArray(record.getPayloadContent());
      key = UrlUtils.urlToKey(url);
      if (uri.equals(url)) {
        System.out.println("-----------------------------------");
        System.out.println("Found at " + inputArcFile.getName());
        System.out.println(date);
        System.out.println(type);
        System.out.println(content.length);
        System.out.println(key);
      }
    }
    reader.close();
    in.close();
  }

  private void searchWarcFile(File inputWarcFile, String uri) throws FileNotFoundException,
      IOException {
    WarcRecord warcRecord = null;
    String url = null;
    String date = null;
    String type = null;
    byte[] content = null;
    String key = null;

    GZIPInputStream gzInputStream = new GZIPInputStream(new FileInputStream(inputWarcFile));
    ByteCountingPushBackInputStream pbin = new ByteCountingPushBackInputStream(
        new BufferedInputStream(gzInputStream, 8192), 32);
    WarcReader warcReader = WarcReaderFactory.getReaderUncompressed(pbin);

    if (warcReader == null) {
      LOG.info("Can't read warc file " + inputWarcFile.getName());
      return;
    }

    warcReader.setWarcTargetUriProfile(uriProfile);
    warcReader.setBlockDigestEnabled(bBlockDigestEnabled);
    warcReader.setPayloadDigestEnabled(bPayloadDigestEnabled);
    warcReader.setRecordHeaderMaxSize(recordHeaderMaxSize);
    warcReader.setPayloadHeaderMaxSize(payloadHeaderMaxSize);

    while ((warcRecord = warcReader.getNextRecord()) != null) {
      url = warcRecord.header.warcTargetUriStr;
      key = UrlUtils.urlToKey(url);
      Payload payload = warcRecord.getPayload();
      HttpHeader httpHeader = null;
      InputStream payloadStream = null;
      if (payload == null) {
        continue;
      }
      httpHeader = warcRecord.getHttpHeader();
      if (httpHeader != null) {
        payloadStream = httpHeader.getPayloadInputStream();
        type = httpHeader.contentType;
      } else {
        payloadStream = payload.getInputStreamComplete();
      }
      if (payloadStream == null) {
        continue;
      }
      date = warcRecord.header.warcDateStr;
      content = IOUtils.toByteArray(payloadStream);
      if (uri.equals(url)) {
        System.out.println("-----------------------------------");
        System.out.println("Found at " + inputWarcFile.getName());
        System.out.println(date);
        System.out.println(type);
        System.out.println(content.length);
        System.out.println(key);
      }

    }
  }

  private void searchFolder(File inputFolder, String uri) throws IOException {
    long startTime = System.currentTimeMillis();

    GZIPInputStream gzInputStream = null;

    for (int i = 0; i < inputFolder.listFiles().length; i++) {
      File inputFile = inputFolder.listFiles()[i];
      LOG.info("processing file " + i + ": " + inputFile.getName());
      if (inputFile.toString().toLowerCase().endsWith(".gz")) {
        gzInputStream = new GZIPInputStream(new FileInputStream(inputFile));
        ByteCountingPushBackInputStream in = new ByteCountingPushBackInputStream(gzInputStream, 32);
        if (ArcReaderFactory.isArcFile(in)) {
          searchArcFile(inputFile, uri);
        } else if (WarcReaderFactory.isWarcFile(in)) {
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

    SearchForUrl search = new SearchForUrl();
    search.searchFolder(inputFolder, uri);
  }
}
