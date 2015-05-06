package org.warcbase.pig.piggybank;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;

import org.apache.pig.data.TupleFactory;
import org.junit.Test;

public class ExtractTopLevelDomainTest {
  private TupleFactory tupleFactory = TupleFactory.getInstance();

  private static final String[][] CASES1 = {
    {"http://www.umiacs.umd.edu/~jimmylin/", "www.umiacs.umd.edu"},
    {"https://github.com/lintool", "github.com"},
    {"http://ianmilligan.ca/2015/05/04/iipc-2015-slides-for-warcs-wats-and-wgets-presentation/", "ianmilligan.ca"},
    {"index.html", null},
  };

  private static final String[][] CASES2 = {
    {"index.html","http://www.umiacs.umd.edu/~jimmylin/", "www.umiacs.umd.edu"},
    {"index.html","lintool/", null},
  };

  @Test
  public void test1() throws IOException {
    ExtractTopLevelDomain udf = new ExtractTopLevelDomain();

    for (int i = 0; i < CASES1.length; i++) {
      assertEquals(CASES1[i][1], udf.exec(tupleFactory.newTuple(CASES1[i][0])));
    }
  }

  @Test
  public void test2() throws IOException {
    ExtractTopLevelDomain udf = new ExtractTopLevelDomain();

    for (int i = 0; i < CASES2.length; i++) {
      assertEquals(CASES2[i][2],
          udf.exec(tupleFactory.newTuple(Arrays.asList(CASES2[i][0], CASES2[i][1]))));
    }
  }
}
