package org.warcbase.wayback;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.util.logging.Logger;

import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.util.ArchiveUtils;
import org.archive.wayback.ResourceStore;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.core.Resource;
import org.archive.wayback.exception.ResourceNotAvailableException;
import org.archive.wayback.resourcestore.resourcefile.ResourceFactory;

public class WarcbaseResourceStore implements ResourceStore {
  private static final Logger LOGGER = Logger.getLogger(WarcbaseResourceStore.class.getName());

  // Set from bean.
  private String host;
  private int port;
  private String table;

  @Override
  public Resource retrieveResource(CaptureSearchResult result) throws ResourceNotAvailableException {
    Resource r = null;
    String resourceUrl = "http://" + host + ":" + port + "/" + table + "/"
        + ArchiveUtils.get14DigitDate(result.getCaptureDate()) + "/" + result.getOriginalUrl();
    LOGGER.info("Fetching resource url: " + resourceUrl);

    try {
      // Note that this hard codes ARC records, which we need to fix later.
      ARCReader reader = (ARCReader) ARCReaderFactory.get(resourceUrl.toString(),
          new BufferedInputStream(new URL(resourceUrl).openStream()), false);
      r = ResourceFactory.ARCArchiveRecordToResource(reader.get(), reader);
    } catch (IOException e) {
      throw new ResourceNotAvailableException("Error reading " + resourceUrl);
    }

    if (r == null) {
      throw new ResourceNotAvailableException("Unable to find: " + result.toString());
    }

    return r;
  }

  public static byte[] getAsByteArray(URL url) throws IOException {
    URLConnection connection = url.openConnection();
    // Since you get a URLConnection, use it to get the InputStream
    InputStream in = connection.getInputStream();
    // Now that the InputStream is open, get the content length
    int contentLength = connection.getContentLength();

    // To avoid having to resize the array over and over and over as
    // bytes are written to the array, provide an accurate estimate of
    // the ultimate size of the byte array
    ByteArrayOutputStream tmpOut;
    if (contentLength != -1) {
      tmpOut = new ByteArrayOutputStream(contentLength);
    } else {
      tmpOut = new ByteArrayOutputStream(16384); // Pick some appropriate
      // size
    }

    byte[] buf = new byte[512];
    while (true) {
      int len = in.read(buf);
      if (len == -1) {
        break;
      }
      tmpOut.write(buf, 0, len);
    }
    in.close();
    tmpOut.close();
    // No effect, but good to do anyway to keep the metaphor alive

    return tmpOut.toByteArray();
  }

  @Override
  public void shutdown() throws IOException {}

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }
}
