package org.warcbase.analysis;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.log4j.Logger;
import org.warcbase.data.Util;

import tl.lin.data.SortableEntries.Order;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfObjectInt;

public class CountRowTypes {
  private static final Logger LOG = Logger.getLogger(CountRowTypes.class);

  private static final String NAME_OPTION = "name";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("name").hasArg()
        .withDescription("name of the archive").create(NAME_OPTION));

    Configuration hbaseConfig = HBaseConfiguration.create();

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(NAME_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(CountRowTypes.class.getCanonicalName(), options);
      System.exit(-1);
    }
    String name = cmdline.getOptionValue(NAME_OPTION);

    int count = 0;
    HTable table = new HTable(hbaseConfig, name);

    LOG.info("Scanning full table...");
    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());
    ResultScanner scanner = null;
    scanner = table.getScanner(scan);

    Object2IntFrequencyDistribution<String> fileTypeCounter = new Object2IntFrequencyDistributionEntry<String>();
    Object2IntFrequencyDistribution<String> domainCounter = new Object2IntFrequencyDistributionEntry<String>();

    int cnt = 0;
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      byte[] key = rr.getRow();
      String url = new String(key, "UTF8");
      count++;
      domainCounter.increment(Util.getDomain(url));

      String fileType = Util.getFileType(url);
      if (fileType.equals("")) {
        fileType = "unknown";
      }
      fileTypeCounter.increment(fileType);

      cnt++;
      if (cnt % 10000 == 0) {
        LOG.info(cnt + " rows scanned");
      }
    }
    LOG.info("Done!");
    table.close();

    System.out.println("Number of rows in the table overall: " + count);
    System.out.println("\nBreakdown by file type:");
    for (PairOfObjectInt<String> entry : fileTypeCounter.getEntries(Order.ByRightElementDescending)) {
      System.out.println(entry.getLeftElement() + " " + entry.getRightElement());
    }
    System.out.println("\nBreakdown by domain:");
    for (PairOfObjectInt<String> entry : domainCounter.getEntries(Order.ByRightElementDescending)) {
      System.out.println(entry.getLeftElement() + " " + entry.getRightElement());
    }
  }
}
