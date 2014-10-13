package org.warcbase.data;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.log4j.Logger;
import org.archive.io.warc.WARCRecord;

/**
 * Utilities for working with {@code WARCRecord}s (from archive.org APIs).
 */
public class WarcRecordUtils {
  private static final Logger LOG = Logger.getLogger(WarcRecordUtils.class);

  // TODO: these methods work fine, but there's a lot of unnecessary buffer copying, which is
  // terrible from a performance perspective.

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
   * Extracts raw contents from an {@code WARCRecord} (including HTTP headers).
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
