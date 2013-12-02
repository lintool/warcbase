package org.warcbase.pig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.io.Files;
import com.google.common.io.Resources;

public class TestArcLoaderPig {
  private static final Log LOG = LogFactory.getLog(TestArcLoaderPig.class);
  private File tempDir;

  @Test
  public void testCountLinks() throws Exception {
    String arcTestDataFile = Resources.getResource("arc/example.arc.gz").getPath();

    String pigFile = Resources.getResource("scripts/TestCountLinks.pig").getPath();
    String location = tempDir.getPath().replaceAll("\\\\", "/"); // make it work on windows

    PigTest test = new PigTest(pigFile, new String[] { "testArcFolder=" + arcTestDataFile,
        "experimentfolder=" + location });

    Iterator<Tuple> parses = test.getAlias("a");

    int cnt = 0;
    while (parses.hasNext()) {
      LOG.info("link and anchor text: " + parses.next());
      cnt++;
    }
    assertEquals(659, cnt);
  }

  @Test
  public void testArcLoader() throws Exception {
    String arcTestDataFile = Resources.getResource("arc/example.arc.gz").getPath();

    String pigFile = Resources.getResource("scripts/TestArcLoader.pig").getPath();
    String location = tempDir.getPath().replaceAll("\\\\", "/"); // make it work on windows

    PigTest test = new PigTest(pigFile, new String[] { "testArcFolder=" + arcTestDataFile,
        "experimentfolder=" + location });

    Iterator<Tuple> parses = test.getAlias("c");

    Tuple tuple = parses.next();
    assertEquals("20080430", (String) tuple.get(0));
    assertEquals(300L, (long) (Long) tuple.get(1));

    // There should only be one record.
    assertFalse(parses.hasNext());
  }

  @Before
  public void setUp() throws Exception {
    // create a random file location
    tempDir = Files.createTempDir();
    LOG.info("Output can be found in " + tempDir.getPath());
  }

  @After
  public void tearDown() throws Exception {
    // cleanup
    FileUtils.deleteDirectory(tempDir);
    LOG.info("Removing tmp files in " + tempDir.getPath());
  }
}
