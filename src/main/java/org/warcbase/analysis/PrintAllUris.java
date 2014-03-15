package org.warcbase.analysis;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.warcbase.browser.WarcBrowser;
import org.warcbase.data.Util;

import tl.lin.data.SortableEntries.Order;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfObjectInt;

public class PrintAllUris {
  private static final Logger LOG = Logger.getLogger(PrintAllUris.class);
  private static final String NAME_OPTION = "name";
  private static final String PORT_OPTION = "port";
  private static final String SERVER_OPTION = "server";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("name").hasArg()
        .withDescription("name of the archive").create(NAME_OPTION));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("port to serve on")
        .create(PORT_OPTION));
    options.addOption(OptionBuilder.withArgName("url").hasArg().withDescription("server prefix")
        .create(SERVER_OPTION));

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

    if (!cmdline.hasOption(PORT_OPTION) || !cmdline.hasOption(SERVER_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(WarcBrowser.class.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }
    int port = Integer.parseInt(cmdline.getOptionValue(PORT_OPTION));
    String server = cmdline.getOptionValue(SERVER_OPTION);

    HTablePool pool = new HTablePool();

    HTableInterface table = pool.getTable(name);
    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());
    ResultScanner scanner = null;
    scanner = table.getScanner(scan);
    Object2IntFrequencyDistribution<String> domainCounter = new Object2IntFrequencyDistributionEntry<String>();
    String url = null;
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      byte[] key = rr.getRow();
      url = new String(key, "UTF8");
      url = Util.getDomain(url);
      url = Util.reverseBacHostnamek(url);
      domainCounter.increment(url);
    }

    PrintWriter writer = new PrintWriter("urls.html", "UTF-8");
    writer.println("<html>");
    writer.println("<body>");
    for (PairOfObjectInt<String> entry : domainCounter.getEntries(Order.ByRightElementDescending)) {
      writer.println("<a href=\"" + server + name + "?query=" + entry.getLeftElement() + "/"
          + "\">" + entry.getLeftElement() + "</a><br/>");
    }
    writer.println("</body>");
    writer.println("</html>");
    writer.close();
    pool.close();
  }
}
