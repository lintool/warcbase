package org.warcbase;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

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
import org.apache.hadoop.hbase.mapreduce.RowCounter;

public class Dashboard {
  
  private static final String N_OPTION = "n";
  public static Configuration hbaseConfig = null;
  public static HTable table = null;
  
  static {
    hbaseConfig = HBaseConfiguration.create();
  }
  
  public static String getFileType(String url){
    //System.out.println(url);
    if(url.length() > 0 && url.charAt(url.length() - 1) == '/')
      return "";
    String[] splits = url.split("\\/");
    if(splits.length == 0)
      return "";
    splits = splits[splits.length - 1].split("\\.");
    //System.out.println(splits.length);
    if(splits.length <= 1)
      return "";
    String type = splits[splits.length - 1];
    if(type.length() > 8)
      return "";
    if(type.length() == 1 && Character.isDigit(type.charAt(0)))
        return "";
    return type;
  }
  
  public static String getDomain(String url){
    String[] splits = url.split("\\/");
    return splits[0];
  }
  
  public static Map<String, Integer> sortByValue(HashMap<String, Integer> map) {
    List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(map.entrySet());

    Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {

        public int compare(Map.Entry<String, Integer> m1, Map.Entry<String, Integer> m2) {
            return (m2.getValue()).compareTo(m1.getValue());
        }
    });

    Map<String, Integer> result = new LinkedHashMap<String, Integer>();
    for (Map.Entry<String, Integer> entry : list) {
        result.put(entry.getKey(), entry.getValue());
    }
    return result;
}
  
  public static void main(String[] args) throws IOException {
    /*String testString = "com.89north.www/wp-content/plugins/jquery-drop-down-menu-plugin/noConflict.js?ver=3.5.1";
    System.out.println(getDomain(testString));
    if(true)
      return;*/
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("n").hasArg()
        .withDescription("Num").create(N_OPTION));
    
    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }
    if (!cmdline.hasOption(N_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(LoadWARC.class.getCanonicalName(), options);
      System.exit(-1);
    }
    int num = Integer.parseInt(cmdline.getOptionValue(N_OPTION));
    
    int count = 0;
    try {
      table = new HTable(hbaseConfig, Constants.TABLE_NAME);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
        
    System.out.println("scanning full table:");
    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());
    ResultScanner scanner = null;
    try {
      scanner = table.getScanner(scan);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    //RowCounter rCounter = new RowCounter();
    //rCounter.se
    
    HashMap<String, Integer> fileTypeCounter = new HashMap<String, Integer>();
    HashMap<String, Integer> domainCounter = new HashMap<String, Integer>();
    
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      byte[] key = rr.getRow();
      String url = new String(key, "UTF8");
      count++;
      String domain = getDomain(url);
      if(domainCounter.containsKey(domain))
        domainCounter.put(domain, domainCounter.get(domain) + 1);
      else
        domainCounter.put(domain, 1);
      String fileType = getFileType(url);
      if(fileType.equals(""))
        continue;
      if(fileTypeCounter.containsKey(fileType))
        fileTypeCounter.put(fileType, fileTypeCounter.get(fileType) + 1);
      else
        fileTypeCounter.put(fileType, 1);
      //System.out.println(new String(key, "UTF8") + " " + getFileType(url));
    }
    System.out.println("Number of rows in the table overall: " + count);
    System.out.println("\nBreakdown by file type:");
    Map<String, Integer> sortedMap = sortByValue(fileTypeCounter);
    int i = 0;
    for(Map.Entry<String, Integer> entry: sortedMap.entrySet()){
      System.out.println(entry.getKey() + " " + entry.getValue());
      if(i > num)
        break;
      i++;
    }
    System.out.println("\nBreakdown by domain:");
    Map<String, Integer> sortedDomain = sortByValue(domainCounter);
    i = 0;
    for(Map.Entry<String, Integer> entry: sortedDomain.entrySet()){
      System.out.println(entry.getKey() + " " + entry.getValue());
      if(i > num)
        break;
      i++;
    }
  }
}
