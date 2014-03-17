package org.warcbase.ingest;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
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
import org.warcbase.data.HbaseManager;
import org.warcbase.data.Util;

public class IngestFiles {
  private static final String CREATE_OPTION = "create";
  private static final String APPEND_OPTION = "append";
  private static final String NAME_OPTION = "name";
  private static final String DIR_OPTION = "dir";
  private static final String START_OPTION = "start";

  private static final Logger LOG = Logger.getLogger(IngestFiles.class);
  // TODO: rename to constants and make final
  private static final UriProfile uriProfile = UriProfile.RFC3986_ABS_16BIT_LAX;
  private static final boolean bBlockDigestEnabled = true;
  private static final boolean bPayloadDigestEnabled = true;
  private static final int recordHeaderMaxSize = 8192;
  private static final int payloadHeaderMaxSize = 32768;

  public static final int MAX_CONTENT_SIZE = 1024 * 1024;

  private int cnt = 0;
  private int skipped = 0;

  private final HbaseManager hbaseManager;

  public IngestFiles(String name, boolean create) throws Exception {
    hbaseManager = new HbaseManager(name, create);
  }

  private void ingestArcFile(File inputArcFile) throws IOException {
    ArcRecordBase record = null;
    String url = null;
    String date = null;
    byte[] content = null;
    String type = null;
    String key = null;

    InputStream in = new FileInputStream(inputArcFile);
    ArcReader reader = ArcReaderFactory.getReader(in);
    while ((record = reader.getNextRecord()) != null) {
      try {
        url = record.getUrlStr();
        date = record.getArchiveDateStr();
        content = IOUtils.toByteArray(record.getPayloadContent());
        key = Util.reverseHostname(url);
        type = record.getContentTypeStr();

        if (key != null && type == null) {
          type = "text/plain";
        }

        if (key == null) {
          continue;
        }

        if (content.length > MAX_CONTENT_SIZE) {
          skipped++;
        } else {
          if (cnt % 10000 == 0 && cnt > 0) {
            LOG.info("Ingested " + cnt + " records into Hbase.");
          }
          if (hbaseManager.addRecord(key, date, content, type)) {
            cnt++;
          } else {
            skipped++;
          }
        }
      } catch (Exception e) {
        LOG.error("Error ingesting record: " + e);
      }
    }

    reader.close();
    in.close();
  }

  private void ingestWarcFile(File inputWarcFile) throws IOException {
    WarcRecord warcRecord = null;
    String uri = null;
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
      uri = warcRecord.header.warcTargetUriStr;
      key = Util.reverseHostname(uri);
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
        skipped++;
        continue;
      }

      date = warcRecord.header.warcDateStr;

      if (payloadStream.available() > MAX_CONTENT_SIZE) {
        skipped++;
        continue;
      }
      content = IOUtils.toByteArray(payloadStream);
      // TODO: fix this
      if (key == null) {
        skipped++;
        continue;
      }

      if (type == null) {
        type = "text/plain";
      }

      if (warcRecord.getHeader("WARC-Type").value.toLowerCase().equals("response")) {
        if (content.length > MAX_CONTENT_SIZE) {
          skipped++;
          continue;
        }
        if (cnt % 10000 == 0 && cnt > 0) {
          LOG.info("Ingested " + cnt + " records into Hbase.");
        }
        if (hbaseManager.addRecord(key, date, content, type)) {
          cnt++;
        } else {
          skipped++;
        }
      }
    }

    warcReader.close();
    pbin.close();
    gzInputStream.close();
  }

  private void ingestFolder(File inputFolder, int i) throws IOException {
    long startTime = System.currentTimeMillis();
    cnt = 0;
    skipped = 0;
    GZIPInputStream gzInputStream = null;

    for (; i < inputFolder.listFiles().length; i++) {
      File inputFile = inputFolder.listFiles()[i];
      if (!(inputFile.getName().endsWith(".warc.gz") || inputFile.getName().endsWith(".arc.gz")
          || inputFile.getName().endsWith(".warc") || inputFile.getName().endsWith(".arc"))) {
        continue;
      }

      LOG.info("processing file " + i + ": " + inputFile.getName());

      if (inputFile.toString().toLowerCase().endsWith(".gz")) {
        gzInputStream = new GZIPInputStream(new FileInputStream(inputFile));
        ByteCountingPushBackInputStream in = new ByteCountingPushBackInputStream(gzInputStream, 32);
        if (ArcReaderFactory.isArcFile(in)) {
          ingestArcFile(inputFile);
        } else if (WarcReaderFactory.isWarcFile(in)) {
          ingestWarcFile(inputFile);
        }
      }
    }

    long totalTime = System.currentTimeMillis() - startTime;
    LOG.info("Total " + cnt + " records inserted, " + skipped + " records skipped");
    LOG.info("Total time: " + totalTime + "ms");
    LOG.info("Ingest rate: " + cnt / (totalTime / 1000) + " records per second.");
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
      System.err.println(String.format("Must specify either -%s or -%s", CREATE_OPTION,
          APPEND_OPTION));
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
