package org.warcbase.analysis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.util.Version;
import org.warcbase.analysis.ner.NEFinder;
import org.warcbase.data.UrlMapping;
import org.warcbase.data.UrlUtils;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class ExtractText {
  private static final Logger LOG = Logger.getLogger(ExtractText.class);
  private static final String HB_OPTION = "hbName";
  private static final String ID_URL_OPTION = "idUrl";
  private static final String URL_MAPPING_OPTION = "urlMapping";
  private static final String OUTPUT_OPTION = "outputDir";
  private static final String STOPWORD_OPTION = "stopWord";
  private static final String NEFINDER_OPTION = "neFinder";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException, NoSuchAlgorithmException {

    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("hbName").hasArg()
        .withDescription("name of the archive (hbtable name)").create(HB_OPTION));

    options.addOption(OptionBuilder.withArgName("idUrl").hasArg()
        .withDescription("id_url CSV file").create(ID_URL_OPTION));

    options.addOption(OptionBuilder.withArgName("urlMapping").hasArg()
        .withDescription("FST mapping file").create(URL_MAPPING_OPTION));

    options.addOption(OptionBuilder.withArgName("outputDir").hasArg()
        .withDescription("output dir for text files").create(OUTPUT_OPTION));

    options.addOption(OptionBuilder.withArgName("stopWord").hasArg()
        .withDescription("stop word file").create(STOPWORD_OPTION));

    options
        .addOption(OptionBuilder
            .withArgName("neFinder")
            .hasArg()
            .withDescription(
                "english stanford name entity reg (english.all.3class.distsim.crf.ser.gz)")
            .create(NEFINDER_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(ID_URL_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)
        || !cmdline.hasOption(HB_OPTION) || !cmdline.hasOption(URL_MAPPING_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(ExtractText.class.getCanonicalName(), options);
      System.exit(-1);
    }
    String hbName = cmdline.getOptionValue(HB_OPTION);
    String outputDir = cmdline.getOptionValue(OUTPUT_OPTION);
    String senateIdList = cmdline.getOptionValue(ID_URL_OPTION);
    String fstMappingFile = cmdline.getOptionValue(URL_MAPPING_OPTION);

    String stopWordFile = "";
    boolean useStopWord = false;
    if (cmdline.hasOption(STOPWORD_OPTION)) {
      stopWordFile = cmdline.getOptionValue(STOPWORD_OPTION);
      useStopWord = true;
    }

    String neFile = "";
    boolean useNEFinder = false;
    if (cmdline.hasOption(NEFINDER_OPTION)) {
      neFile = cmdline.getOptionValue(NEFINDER_OPTION);
      useNEFinder = true;
    }

    UrlMapping mapping = new UrlMapping(fstMappingFile);

    // init ne finder
    NEFinder neFinder = null;
    if (useNEFinder) {
      neFinder = new NEFinder(neFile);
    }

    // read id-uri
    Map<String, String> idUri = new HashMap<String, String>();
    try {
      String line = "";
      BufferedReader br = new BufferedReader(new FileReader(senateIdList));
      while ((line = br.readLine()) != null) {
        String[] splits = line.split(",");
        for (int i = 1; i < splits.length; i++) {
          String uri = splits[i];
          while (uri.startsWith(" ")) {
            uri = uri.substring(1);
          }
          idUri.put(uri, splits[0]);
        }
      }
      br.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    List<String> ids = new ArrayList<String>(100);
    for (Map.Entry<String, String> entry : idUri.entrySet()) {
      if (!ids.contains(entry.getValue())) {
        ids.add(entry.getValue());
      }
    }

    // Creating stop_words list
    ArrayList<String> stop_words = new ArrayList<String>();
    if (useStopWord) {
      try {
        BufferedReader reader = new BufferedReader(new FileReader(stopWordFile));
        String line = null;
        while ((line = reader.readLine()) != null) {
          stop_words.add(line);
        }
        reader.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    // read hb table
    HTablePool pool = new HTablePool();
    HTableInterface table = pool.getTable(hbName);
    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());
    ResultScanner scanner = null;
    scanner = table.getScanner(scan);

    String type = "";
    String content = "";
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      byte[] key = rr.getRow();
      String id = "";
      String keyStr = Bytes.toString(key);
      boolean ambiguous = false;
      for (Map.Entry<String, String> entry : idUri.entrySet()) {
        if (keyStr.startsWith(entry.getKey())) {
          if (!id.equals("") && !id.equals(entry.getValue())) {
            LOG.warn(id + " " + entry.getValue());
            ambiguous = true;
          }
          id = entry.getValue();
        }
      }
      // id doesn't exist in the htable
      // go to the next id
      if (id.equals("") || ambiguous) {
        continue;
      }

      if (id.equals("dole")) {
        String[] splits = keyStr.split("\\/");
        if (splits.length > 3 || Math.random() < 0.3) {
          continue;
        }
      }

      if (id.equals("frist")) {
        String[] splits = keyStr.split("\\/");
        if (splits.length > 3 || Math.random() < 0.25) {
          continue;
        }
      }

      if (id.equals("harkin")) {
        String[] splits = keyStr.split("\\/");
        if (splits.length > 3 || Math.random() < 0.5) {
          continue;
        }
      }

      if (id.equals("hatch")) {
        String[] splits = keyStr.split("\\/");
        if (splits.length > 3 || Math.random() < 0.45) {
          continue;
        }
      }

      if (id.equals("specter")) {
        String[] splits = keyStr.split("\\/");
        if (splits.length > 3 || Math.random() < 0.4) {
          continue;
        }

      }

      /**
       * 
       */
      String uri = UrlUtils.keyToUrl(keyStr);

      int mapId = mapping.getID(uri);
      // System.out.println(keyStr + " " + uri + " " + mapId);

      Get get = new Get(key);
      Result rs = table.get(get);

      if (rs.raw().length == 0) {
        continue;
      }

      type = Bytes.toString(rs.raw()[0].getQualifier());

      if (!(type.contains("html"))) {
        continue;
      }

      for (int i = 0; i < rs.raw().length; i++) {
        content = Bytes.toString(rs.raw()[i].getValue());

        String cleaned = ExtractTextUtil.getText(content).replaceAll("[\\r\\n]+", " ");
        StringTokenizer st = new StringTokenizer(cleaned);
        List<String> words = Lists.newArrayList();
        while (st.hasMoreElements()) {
          words.add((String) st.nextElement());
        }
        if (useStopWord) {
          words.removeAll(stop_words);
        }
        cleaned = Joiner.on(" ").join(words);

        if (useNEFinder) {
          cleaned = neFinder.replaceNER(cleaned);
        }
        words = AnalyzerUtils.parse(new SimpleAnalyzer(Version.LUCENE_45), cleaned);
        if (useStopWord) {
          words.removeAll(stop_words);
        }

        String text = Joiner.on(" ").join(words);
        if (text.length() < 300)
          continue;
        String fileName = outputDir + "/" + id + "#" + mapId + "#" + rs.raw()[i].getTimestamp()
            + ".txt";
        if (text.contains("this page cannot be found sorry")
            || text.contains("this page can not be found sorry")) {
          continue;
        }
        FileWriter out = new FileWriter(fileName, true);
        out.write(text);
        out.close();
        // for debugging
        // fileName = outputDir + "/" + id + "#" + mapId + "#" + rs.raw()[i].getTimestamp() +
        // ".html";
        // out = new FileWriter(fileName, true);
        // out.write(content);
        // out.close();

      }
    }

    pool.close();

  }

}
