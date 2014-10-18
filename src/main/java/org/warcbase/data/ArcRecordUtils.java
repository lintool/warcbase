package org.warcbase.data;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;

/**
 * Utilities for working with {@code ARCRecord}s (from archive.org APIs).
 */
public class ArcRecordUtils {
  private static final Logger LOG = Logger.getLogger(ArcRecordUtils.class);

  // TODO: these methods work fine, but there's a lot of unnecessary buffer copying, which is
  // terrible from a performance perspective.

  /**
   * Converts raw bytes into an {@code ARCRecord}.
   *
   * @param bytes raw bytes
   * @return parsed {@code ARCRecord}
   * @throws IOException
   */
  public static ARCRecord fromBytes(byte[] bytes) throws IOException {
    ARCReader reader = (ARCReader) ARCReaderFactory.get("",
        new BufferedInputStream(new ByteArrayInputStream(bytes)), false);
    return (ARCRecord) reader.get();
  }

  public static byte[] toBytes(ARCRecord record) throws IOException {
    ARCRecordMetaData meta = record.getMetaData();

    String metaline = meta.getUrl() + " " + meta.getIp() + " " + meta.getDate() + " "
        + meta.getMimetype() + " " + (int) meta.getLength();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(baos);
    dout.write(metaline.getBytes());
    dout.write("\n".getBytes());
    copyStream(record, (int) meta.getLength(), true, dout);

    return baos.toByteArray();
  }

  /**
   * Extracts raw contents from an {@code ARCRecord} (including HTTP headers).
   *
   * @param record the {@code ARCRecord}
   * @return raw contents
   * @throws IOException
   */
  public static byte[] getContent(ARCRecord record) throws IOException {
    ARCRecordMetaData meta = record.getMetaData();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dout = new DataOutputStream(baos);
    copyStream(record, (int) meta.getLength(), true, dout);

    return baos.toByteArray();
  }

  /**
   * Extracts contents of the body from an {@code ARCRecord} (excluding HTTP headers).
   *
   * @param record the {@code ARCRecord}
   * @return contents of the body
   * @throws IOException
   */
  public static byte[] getBodyContent(ARCRecord record) throws IOException {
    byte[] raw = getContent(record);
    int bodyOffset = record.getBodyOffset();

    byte[] content = new byte[raw.length - bodyOffset];
    System.arraycopy(raw, bodyOffset, content, 0, content.length);
    
    return content;
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
