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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecordBase;

import com.google.common.io.Resources;

public class JwatArcLoaderTest {
  private static final Log LOG = LogFactory.getLog(JwatArcLoaderTest.class);

  @Test
  public void testCountLinks() throws Exception {
    String arcTestDataFile = Resources.getResource("arc/example.arc.gz").getPath();
    
    ArcRecordBase record = null;
    InputStream in = new FileInputStream(new File(arcTestDataFile));
    ArcReader reader = ArcReaderFactory.getReader(in);
    int cnt = 0;
    while ((record = reader.getNextRecord()) != null) {
      // This is how you get out various fields.
      @SuppressWarnings("unused") String url = record.getUrlStr();
      @SuppressWarnings("unused") String date = record.getArchiveDateStr();
      @SuppressWarnings("unused") String content = "";
      String type = record.getContentTypeStr();

      if (type.toLowerCase().contains("text")) {
        content = new String(IOUtils.toByteArray(record.getPayloadContent()), Charset.forName("UTF-8"));
      }

      cnt++;
    }
    reader.close();
    in.close();

    LOG.info(cnt + " records read!");
    assertEquals(300, cnt);
  }
}
