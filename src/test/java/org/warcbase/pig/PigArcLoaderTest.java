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

public class PigArcLoaderTest {
  private static final Log LOG = LogFactory.getLog(PigArcLoaderTest.class);
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
    assertEquals(664, cnt);
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
    assertEquals("20080430", tuple.get(0));
    assertEquals(300L, (long) tuple.get(1));

    // There should only be one record.
    assertFalse(parses.hasNext());
  }

  @Test
  public void testDetectLanguage() throws Exception {
    String arcTestDataFile;
    arcTestDataFile = Resources.getResource("arc/example.arc.gz").getPath();

    String pigFile = Resources.getResource("scripts/TestDetectLanguage.pig").getPath();
    String location = tempDir.getPath().replaceAll("\\\\", "/"); // make it work on windows

    PigTest test = new PigTest(pigFile, new String[] { "testArcFolder=" + arcTestDataFile, "experimentfolder=" + location });

    Iterator<Tuple> parses = test.getAlias("d");

    while (parses.hasNext()) {
      Tuple tuple = parses.next();
      String lang = (String) tuple.get(0);
      switch (lang) {
        case "en": assertEquals(57L, (long) tuple.get(1)); break;
        case "et": assertEquals( 6L, (long) tuple.get(1)); break;
        case "it": assertEquals( 1L, (long) tuple.get(1)); break;
        case "lt": assertEquals(66L, (long) tuple.get(1)); break;
        case "no": assertEquals( 6L, (long) tuple.get(1)); break;
        case "ro": assertEquals( 4L, (long) tuple.get(1)); break;
      }
      System.out.println("language test: " + tuple.getAll());
    }

  }

  /*
   * The two tests of MIME type detection is dependent on the version of the corresponding Tika and magiclib libraries
   */

  //@Test
  // Commenting out this test case for now since it requires a 3rd party lib to be installed.
  public void testDetectMimeTypeMagic() throws Exception {
    String arcTestDataFile;
    arcTestDataFile = Resources.getResource("arc/example.arc.gz").getPath();

    String pigFile = Resources.getResource("scripts/TestDetectMimeTypeMagic.pig").getPath();
    String location = tempDir.getPath().replaceAll("\\\\", "/"); // make it work on windows ?

    PigTest test = new PigTest(pigFile, new String[] { "testArcFolder=" + arcTestDataFile,
        "experimentfolder=" + location });

    Iterator<Tuple> ts = test.getAlias("magicMimeBinned");
    while (ts.hasNext()) {
      Tuple t = ts.next(); // t = (mime type, count)
      String mime = (String) t.get(0);
      System.out.println(mime + ": " + t.get(1));
      if (mime != null) {
        switch (mime) {
          case                         "EMPTY": assertEquals(  7L, (long) t.get(1)); break;
          case                     "text/html": assertEquals(139L, (long) t.get(1)); break;
          case                    "text/plain": assertEquals( 80L, (long) t.get(1)); break;
          case                     "image/gif": assertEquals( 29L, (long) t.get(1)); break;
          case               "application/xml": assertEquals( 11L, (long) t.get(1)); break;
          case           "application/rss+xml": assertEquals(  2L, (long) t.get(1)); break;
          case         "application/xhtml+xml": assertEquals(  1L, (long) t.get(1)); break;
          case      "application/octet-stream": assertEquals( 26L, (long) t.get(1)); break;
          case "application/x-shockwave-flash": assertEquals(  8L, (long) t.get(1)); break;
        }
      }
    }
  }

  @Test
  public void testDetectMimeTypeTika() throws Exception {
    String arcTestDataFile;
    arcTestDataFile = Resources.getResource("arc/example.arc.gz").getPath();

    String pigFile = Resources.getResource("scripts/TestDetectMimeTypeTika.pig").getPath();
    String location = tempDir.getPath().replaceAll("\\\\", "/"); // make it work on windows ?

    PigTest test = new PigTest(pigFile, new String[] { "testArcFolder=" + arcTestDataFile, "experimentfolder=" + location});

    Iterator <Tuple> ts = test.getAlias("tikaMimeBinned");
    while (ts.hasNext()) {
      Tuple t = ts.next();

      String mime = (String) t.get(0);
      switch (mime) {
        case                     "image/gif": assertEquals( 29L, (long) t.get(1)); break;
        case                     "image/png": assertEquals(  8L, (long) t.get(1)); break;
        case                    "image/jpeg": assertEquals( 18L, (long) t.get(1)); break;
        case                     "text/html": assertEquals(132L, (long) t.get(1)); break;
        case                    "text/plain": assertEquals( 86L, (long) t.get(1)); break;
        case               "application/xml": assertEquals(  2L, (long) t.get(1)); break;
        case           "application/rss+xml": assertEquals(  9L, (long) t.get(1)); break;
        case         "applicaiton/xhtml+xml": assertEquals(  1L, (long) t.get(1)); break;
        case      "application/octet-stream": assertEquals(  7L, (long) t.get(1)); break;
        case "application/x-shockwave-flash": assertEquals(  8L, (long) t.get(1)); break;
      }
      System.out.println(t.get(0) + ": " + t.get(1));
    }
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
