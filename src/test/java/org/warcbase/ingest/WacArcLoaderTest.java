package org.warcbase.ingest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.junit.Test;

import com.google.common.io.Resources;

public class WacArcLoaderTest {
  private static final Log LOG = LogFactory.getLog(WacArcLoaderTest.class);

  @Test
  public void testInputFormat() throws Exception {
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
}
