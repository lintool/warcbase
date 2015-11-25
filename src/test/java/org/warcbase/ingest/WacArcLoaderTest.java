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

package org.warcbase.ingest;

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.junit.Test;
import org.warcbase.data.ArcRecordUtils;

import com.google.common.io.Resources;

public class WacArcLoaderTest {
  private static final Log LOG = LogFactory.getLog(WacArcLoaderTest.class);

  @Test
  public void testReader() throws Exception {
    String[] urls = new String[] {
        "filedesc://IAH-20080430204825-00000-blackbook.arc",
        "dns:www.archive.org",
        "http://www.archive.org/robots.txt",
        "http://www.archive.org/",
        "http://www.archive.org/index.php" };

    String arcFile = Resources.getResource("arc/example.arc.gz").getPath();
    ARCReader reader = ARCReaderFactory.get(new File(arcFile));

    int cnt = 0;
    for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext();) {
      ARCRecord r = (ARCRecord) ii.next();
      ARCRecordMetaData meta = r.getMetaData();

      if (cnt < urls.length) {
        assertEquals(urls[cnt], meta.getUrl());
      }
      cnt++;
    }
    reader.close();

    LOG.info(cnt + " records read!");
    assertEquals(300, cnt);
  }

  @Test
  public void testReadFromStream() throws Exception {
    String arcFile = Resources.getResource("arc/example.arc.gz").getPath();
    ARCReader reader = ARCReaderFactory.get(new File(arcFile));

    int cnt = 0;
    for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext();) {
      ARCRecord r = (ARCRecord) ii.next();
      // Skip the file header.
      if (cnt == 0) {
        cnt++;
        continue;
      }

      String h = r.getHeaderString();
      InputStream in = new DataInputStream(new ByteArrayInputStream(ArcRecordUtils.toBytes(r)));

      ARCReader nr = (ARCReader) ARCReaderFactory.get("",
          new BufferedInputStream(in), false);
      ARCRecord r2 = (ARCRecord) nr.get();

      assertEquals(h, r2.getHeaderString());
      cnt++;
    }
    reader.close();

    LOG.info(cnt + " records read!");
    assertEquals(300, cnt);
  }
}
