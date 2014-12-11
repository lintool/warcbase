package org.warcbase.data;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.commons.httpclient.HttpParser;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCConstants;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;

/**
 * Utilities for working with {@code WARCRecord}s (from archive.org APIs).
 */
public class WarcRecordUtils implements WARCConstants {
  private static final Logger LOG = Logger.getLogger(WarcRecordUtils.class);

  // TODO: these methods work fine, but there's a lot of unnecessary buffer copying, which is
  // terrible from a performance perspective.

  public static WARCRecord fromBytes(byte[] bytes) throws IOException {
    WARCReader reader = (WARCReader) WARCReaderFactory.get("",
        new BufferedInputStream(new ByteArrayInputStream(bytes)), false);
    return (WARCRecord) reader.get();
  }

  public static byte[] toBytes(WARCRecord record) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(baos);

    dout.write(new String("WARC/0.17\n").getBytes());
    for (Map.Entry<String, Object> entry : record.getHeader().getHeaderFields().entrySet()) {
      dout.write(new String(entry.getKey() + ": " + entry.getValue().toString() + "\n").getBytes());
    }
    dout.write(new String("\n").getBytes());
    record.dump(dout);

    return baos.toByteArray();
  }

  /**
   * Extracts the MIME type of WARC response records (i.e., "WARC-Type" is "response").
   * Note that this is different from the "Content-Type" in the WARC header.
   *
   * @param contents raw contents of the WARC response record
   * @return MIME type
   */
  public static String getWarcResponseMimeType(byte[] contents) {
    // This is a somewhat janky way to get the MIME type of the response.
    // Note that this is different from the "Content-Type" in the WARC header.
    Pattern pattern = Pattern.compile("Content-Type: ([^\\s]+)", Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(new String(contents));
    if (matcher.find()) {
      return matcher.group(1).replaceAll(";$", "");
    }

    return null;
  }

  /**
   * Extracts raw contents from a {@code WARCRecord} (including HTTP headers).
   *
   * @param record the {@code WARCRecord}
   * @return raw contents
   * @throws IOException
   */
  public static byte[] getContent(WARCRecord record) throws IOException {
    int len = (int) record.getHeader().getContentLength();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(baos);
    copyStream(record, len, true, dout);

    return baos.toByteArray();
  }

  /**
   * Extracts contents of the body from a {@code WARCRecord} (excluding HTTP headers).
   *
   * @param record the {@code WARCRecord}
   * @return contents of the body
   * @throws IOException
   */
  public static byte[] getBodyContent(WARCRecord record) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    String line = new String(HttpParser.readLine(record, WARC_HEADER_ENCODING));
    if (line == null) {
      return null;
    }

    // Just using parseHeaders to move down input stream to body
    HttpParser.parseHeaders(record, WARC_HEADER_ENCODING);
    record.dump(baos);
    return baos.toByteArray();
  }

  private static long copyStream(final InputStream is, final int recordLength,
      boolean enforceLength, final DataOutputStream out) throws IOException {
    byte [] scratchbuffer = new byte[recordLength];
    int read = 0;
    long tot = 0;
    while ((tot < recordLength) && (read = is.read(scratchbuffer)) != -1) {
      int write = read;
      // never write more than enforced length
      write = (int) Math.min(write, recordLength - tot);
      tot += read;
      out.write(scratchbuffer, 0, write);
    }
    if (enforceLength && tot != recordLength) {
      LOG.error("Read " + tot + " bytes but expected " + recordLength + " bytes. Continuing...");
    }

    return tot;
  }
}
