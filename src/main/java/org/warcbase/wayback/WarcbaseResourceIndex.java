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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.archive.util.ArchiveUtils;
import org.archive.util.io.RuntimeIOException;
import org.archive.util.iterator.CloseableIterator;
import org.archive.wayback.UrlCanonicalizer;
import org.archive.wayback.core.CaptureSearchResult;
import org.archive.wayback.core.CaptureSearchResults;
import org.archive.wayback.core.SearchResult;
import org.archive.wayback.core.WaybackRequest;
import org.archive.wayback.exception.AccessControlException;
import org.archive.wayback.exception.BadQueryException;
import org.archive.wayback.exception.ResourceIndexNotAvailableException;
import org.archive.wayback.exception.ResourceNotInArchiveException;
import org.archive.wayback.resourceindex.LocalResourceIndex;
import org.archive.wayback.resourceindex.filterfactory.AccessPointCaptureFilterGroupFactory;
import org.archive.wayback.resourceindex.filterfactory.AnnotatingCaptureFilterGroupFactory;
import org.archive.wayback.resourceindex.filterfactory.CaptureFilterGroup;
import org.archive.wayback.resourceindex.filterfactory.ClosestTrackingCaptureFilterGroupFactory;
import org.archive.wayback.resourceindex.filterfactory.CoreCaptureFilterGroupFactory;
import org.archive.wayback.resourceindex.filterfactory.ExclusionCaptureFilterGroupFactory;
import org.archive.wayback.resourceindex.filterfactory.FilterGroupFactory;
import org.archive.wayback.resourceindex.filterfactory.QueryCaptureFilterGroupFactory;
import org.archive.wayback.resourceindex.filterfactory.WindowFilterGroup;
import org.archive.wayback.util.ObjectFilter;
import org.archive.wayback.util.ObjectFilterChain;
import org.archive.wayback.util.ObjectFilterIterator;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

public class WarcbaseResourceIndex extends LocalResourceIndex {
  /**
   * maximum number of records to return
   */
  private final static int MAX_RECORDS = 1000;

  private int maxRecords = MAX_RECORDS;

  // Set from bean.
  private UrlCanonicalizer canonicalizer = null;
  private String host;
  private int port;
  private String table;

  private ObjectFilter<CaptureSearchResult> annotater = null;
  private ObjectFilter<CaptureSearchResult> filter = null;
  protected List<FilterGroupFactory> fgFactories = null;

  public WarcbaseResourceIndex() {
    canonicalizer = new AggressiveUrlCanonicalizer();
    fgFactories = new ArrayList<FilterGroupFactory>();
    fgFactories.add(new AccessPointCaptureFilterGroupFactory());
    fgFactories.add(new CoreCaptureFilterGroupFactory());
    fgFactories.add(new QueryCaptureFilterGroupFactory());
    fgFactories.add(new AnnotatingCaptureFilterGroupFactory());
    fgFactories.add(new ExclusionCaptureFilterGroupFactory());
    fgFactories.add(new ClosestTrackingCaptureFilterGroupFactory());
  }

  private void cleanupIterator(CloseableIterator<? extends SearchResult> itr)
      throws ResourceIndexNotAvailableException {
    try {
      itr.close();
    } catch (IOException e) {
      e.printStackTrace();
      throw new ResourceIndexNotAvailableException(e.getLocalizedMessage());
    }
  }

  protected List<CaptureFilterGroup> getRequestFilterGroups(WaybackRequest r)
      throws BadQueryException {
    ArrayList<CaptureFilterGroup> groups = new ArrayList<CaptureFilterGroup>();
    for (FilterGroupFactory f : fgFactories) {
      groups.add(f.getGroup(r, canonicalizer, this));
    }
    return groups;
  }

  public CaptureSearchResults doCaptureQuery(WaybackRequest wbRequest, int type)
      throws ResourceIndexNotAvailableException, ResourceNotInArchiveException, BadQueryException, AccessControlException {
    wbRequest.setResultsPerPage(100);
    String urlKey;
    try {
      urlKey = canonicalizer.urlStringToKey(wbRequest.getRequestUrl());
    } catch (IOException e) {
      throw new BadQueryException("Bad URL(" + wbRequest.getRequestUrl() + ")");
    }

    CaptureSearchResults results = new CaptureSearchResults();
    ObjectFilterChain<CaptureSearchResult> filters = new ObjectFilterChain<CaptureSearchResult>();

    WindowFilterGroup<CaptureSearchResult> window =
        new WindowFilterGroup<CaptureSearchResult>(wbRequest, this);
    List<CaptureFilterGroup> groups = getRequestFilterGroups(wbRequest);
    if (filter != null) {
      filters.addFilter(filter);
    }

    for (CaptureFilterGroup cfg : groups) {
      filters.addFilters(cfg.getFilters());
    }
    filters.addFilters(window.getFilters());

    CloseableIterator<CaptureSearchResult> itr = null;
    try {
      itr = new ObjectFilterIterator<CaptureSearchResult>(
          getIterator(wbRequest.getRequestUrl(), urlKey), filters);

      while (itr.hasNext()) {
        results.addSearchResult(itr.next());
      }
    } catch (RuntimeIOException e) {
      e.printStackTrace();
    } finally {
      if (itr != null) {
        cleanupIterator(itr);
      }
    }

    for (CaptureFilterGroup cfg : groups) {
      cfg.annotateResults(results);
    }

    window.annotateResults(results);

    return results;
  }

  public CloseableIterator<CaptureSearchResult> getIterator(final String url, final String urlKey)
      throws ResourceNotInArchiveException, ResourceIndexNotAvailableException {
    final String resourceUrl = "http://" + host + ":" + port + "/" + table + "/*/" + url;

    List<String> lines = null;
    try {
      byte[] bytes = fetchUrl(new URL(resourceUrl));
      if (bytes.length == 0) {
        throw new ResourceNotInArchiveException("No entries found in: " + resourceUrl);
      }
      lines = Arrays.asList(new String(bytes).split("\n"));
    } catch (MalformedURLException e) {
      throw new ResourceIndexNotAvailableException("Error contacting REST API: " + resourceUrl);
    } catch (IOException e) {
      throw new ResourceIndexNotAvailableException("Error contacting REST API: " + resourceUrl);
    }
    final Iterator<String> it = lines.iterator();

    return new CloseableIterator<CaptureSearchResult>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public CaptureSearchResult next() {
        String line = it.next();
        String[] splits = line.split("\\s+");
        CaptureSearchResult r = new CaptureSearchResult();
        try {
          r.setCaptureDate(ArchiveUtils.parse14DigitDate(splits[0]));
        } catch (ParseException e) {
          e.printStackTrace();
        }
        r.setOriginalUrl(url);
        r.setUrlKey(urlKey);
        // doesn't matter, or we get NPE
        r.setMimeType(splits[1]);
        r.setFile("foo");
        // needed, or otherwise we'll get a NPE in CalendarResults.jsp
        r.setRedirectUrl("-");
        r.setHttpCode("200");
        r.setOffset(0);
        return r;
      }

      @Override public void remove() {}

      @Override public void close() throws IOException {}
    };
  }

  private static byte[] fetchUrl(URL url) throws IOException {
    URLConnection connection = url.openConnection();
    InputStream in = connection.getInputStream();
    int contentLength = connection.getContentLength();

    ByteArrayOutputStream tmpOut;
    if (contentLength != -1) {
      tmpOut = new ByteArrayOutputStream(contentLength);
    } else {
      tmpOut = new ByteArrayOutputStream(16384);
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
    tmpOut.close(); // Does nothing, but good hygiene

    return tmpOut.toByteArray();
  }

  public void setMaxRecords(int maxRecords) {
    this.maxRecords = maxRecords;
  }

  public int getMaxRecords() {
    return maxRecords;
  }

  public UrlCanonicalizer getCanonicalizer() {
    return canonicalizer;
  }

  public void setCanonicalizer(UrlCanonicalizer canonicalizer) {
    this.canonicalizer = canonicalizer;
  }

  public ObjectFilter<CaptureSearchResult> getAnnotater() {
    return annotater;
  }

  public void setAnnotater(ObjectFilter<CaptureSearchResult> annotater) {
    this.annotater = annotater;
  }

  public ObjectFilter<CaptureSearchResult> getFilter() {
    return filter;
  }

  public void setFilter(ObjectFilter<CaptureSearchResult> filter) {
    this.filter = filter;
  }

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
