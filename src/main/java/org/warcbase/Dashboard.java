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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class Dashboard {
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
    System.out.println(getFileType(testString));
    if(true)
      return;*/
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
    HashMap<String, Integer> fileTypeCounter = new HashMap<String, Integer>();
    
    for (Result rr = scanner.next(); rr != null && count < 200; rr = scanner.next()) {
      byte[] key = rr.getRow();
      String url = new String(key, "UTF8");
      count++;
      String fileType = getFileType(url);
      if(fileType.equals(""))
        continue;
      if(fileTypeCounter.containsKey(fileType))
        fileTypeCounter.put(fileType, fileTypeCounter.get(fileType) + 1);
      else
        fileTypeCounter.put(fileType, 1);
      //System.out.println(new String(key, "UTF8") + " " + getFileType(url));
    }
    System.out.println(count);
    Map<String, Integer> sortedMap = sortByValue(fileTypeCounter);
    for(Map.Entry<String, Integer> entry: sortedMap.entrySet()){
      System.out.println(entry.getKey() + " " + entry.getValue());
    }
  }
}
