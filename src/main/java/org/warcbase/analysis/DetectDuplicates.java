package org.warcbase.analysis;

import java.io.IOException;
import java.util.Arrays;

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
import org.warcbase.ingest.IngestFiles;

public class DetectDuplicates {
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
      formatter.printHelp(IngestFiles.class.getCanonicalName(), options);
      System.exit(-1);
    }
    String name = cmdline.getOptionValue(NAME_OPTION);

    HTable table = new HTable(hbaseConfig, name);
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);

    int duplicates = 0;
    long duplicateSize = 0;
    int progress = 0;

    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      progress++;
      for (int i = 0; i < rr.raw().length; i++)
        for (int j = i + 1; j < rr.raw().length; j++) {
          if (rr.raw()[i].getValue().length != rr.raw()[j].getValue().length) {
            continue;
          }
          if (Arrays.equals(rr.raw()[i].getValue(), rr.raw()[j].getValue())) {
            duplicates++;
            duplicateSize += rr.raw()[i].getValue().length;
          }
        }
      if (progress % 10000 == 0) {
        System.out.println("Done with " + progress + " rows. duplicates = " + duplicates);
      }
    }
    table.close();

    System.out.println("Number of Duplicates: " + duplicates);
    System.out.println("Total Duplicate size: " + duplicateSize);
  }
}
