package org.warcbase;

import java.io.IOException;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.hsqldb.lib.Iterator;

public class Dashboard {
  public static Configuration hbaseConfig = null;
  public static HTable table = null;
  
  static {
    hbaseConfig = HBaseConfiguration.create();
  }
  
  public static String getFileType(String url){
    //System.out.println(url);
    String[] splits = url.split("\\.");
    //System.out.println(splits.length);
    if(splits.length <= 1)
      return "";
    return splits[splits.length - 1];
  }
  
  public static void main(String[] args) throws IOException {
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
    HashMap<String, Integer> fileTypeCounter = null;
    
    for (Result rr = scanner.next(); rr != null && count < 200; rr = scanner.next()) {
      byte[] key = rr.getRow();
      String url = new String(key, "UTF8");
      count++;
      if(url.length() > 10)
        url = url.substring(url.length() - 10);
      String fileType = getFileType(url);
      if(fileTypeCounter.containsKey(fileType))
        fileTypeCounter.put(fileType, fileTypeCounter.get(fileType) + 1);
      else
        fileTypeCounter.put(fileType, 1);
      //System.out.println(new String(key, "UTF8") + getFileType(url));
    }
    System.out.println(count);
    SortedSet<String> sortedKeys = new TreeSet<String>(fileTypeCounter.keySet());
    Iterator it = (Iterator) sortedKeys.iterator();
    while(it.hasNext()){
      String value=(String)it.next();
      System.out.println(value + " " + fileTypeCounter.get(value));
    }
  }
}
