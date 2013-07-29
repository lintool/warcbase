package org.warcbase;

import java.io.IOException;

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
  
  public static void getFileType(String url){
    
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
    for (Result rr = scanner.next(); rr != null && count < 200; rr = scanner.next()) {
      byte[] key = rr.getRow();
      count++;
      System.out.println(new String(key, "UTF8"));
    }
    System.out.println(count);
  }
}
