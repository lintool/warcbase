/*
 * Warcbase: an open-source platform for managing web archives
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.warcbase.wayback;

import java.io.IOException;
import java.io.PushbackInputStream;
import java.net.URL;
import java.util.logging.Logger;

import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
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
      // Read first 4 bytes of input stream to detect archive format; push back into stream for re-use
      PushbackInputStream pb = new PushbackInputStream(new URL(resourceUrl).openStream(), 4);
      byte[] signature = new byte[4];
      pb.read(signature, 0, 4);
      pb.unread(signature);

      if ((new String(signature)).equals("WARC")) {
        WARCReader reader = (WARCReader) WARCReaderFactory.get(resourceUrl.toString(), pb, false);
        r = ResourceFactory.WARCArchiveRecordToResource(reader.get(), reader);
      } else {
        // Assume ARC format if not WARC
        ARCReader reader = (ARCReader) ARCReaderFactory.get(resourceUrl.toString(), pb, false);
        r = ResourceFactory.ARCArchiveRecordToResource(reader.get(), reader);
      }
    } catch (IOException e) {

      throw new ResourceNotAvailableException("Error reading " + resourceUrl);
    }

    if (r == null) {
      throw new ResourceNotAvailableException("Unable to find: " + result.toString());
    }

    return r;
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
