package org.warcbase.pig;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.pigunit.PigTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: alan
 */
public class TestArcLoaderPig {

    private static final Log LOG = LogFactory.getLog(TestArcLoaderPig.class);

    private File tempDir;

    @Test
    public void testArcLoader() throws Exception {

        String arcTestDataFile = Resources.getResource("arc/example.arc.gz").getPath();
        //arcTestDataFile = "/home/alan/Documents/SCAPE/hadoop-hackathon-vienna/web/172-3-20131012143440-00001-prepc2.arc.gz";

        String pigFile = Resources.getResource("scripts/TestArcLoader.pig").getPath();
        String location = tempDir.getPath().replaceAll("\\\\", "/");  // make it work on windows

        PigTest test = new PigTest(pigFile, new String[]{
                "testArcFolder=" + arcTestDataFile,
                "experimentfolder=" + location});

        Iterator<Tuple> parses = test.getAlias("c");

        while (parses.hasNext()) {
            System.out.println("date + count in arc file: " + parses.next());
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
        //  FileUtils.deleteRecursive(tempDir);
    }

}
