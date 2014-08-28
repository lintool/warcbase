package org.warcbase.ingest;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.junit.Test;
import org.warcbase.data.WarcRecordUtils;

import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;

import com.google.common.io.Resources;

public class WacWarcLoaderTest {
  private static final Log LOG = LogFactory.getLog(WacWarcLoaderTest.class);

  @Test
  public void testInputFormat() throws Exception {
    String warcFile = Resources.getResource("warc/example.warc.gz").getPath();
    WARCReader reader = WARCReaderFactory.get(new File(warcFile));

    Object2IntFrequencyDistribution<String> types =
        new Object2IntFrequencyDistributionEntry<String>();

    int cnt = 0;
    for (Iterator<ArchiveRecord> ii = reader.iterator(); ii.hasNext();) {
      WARCRecord r = (WARCRecord) ii.next();
      ArchiveRecordHeader header = r.getHeader();

      types.increment((String) header.getHeaderValue("WARC-Type"));

      byte[] contents = WarcRecordUtils.getContent(r);
      int len = (int) header.getContentLength();
      assertEquals(len, contents.length);

      cnt++;
    }
    reader.close();

    LOG.info(cnt + " records read!");
    assertEquals(822, cnt);

    assertEquals(299, types.get("response"));
    assertEquals(1, types.get("warcinfo"));
    assertEquals(261, types.get("request"));
    assertEquals(261, types.get("metadata"));
    assertEquals(4, types.getNumberOfEvents());
    assertEquals(822, types.getSumOfCounts());
  }
}
