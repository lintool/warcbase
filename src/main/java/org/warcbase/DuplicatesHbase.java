package org.warcbase;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class DuplicatesHbase {
  public static Configuration hbaseConfig = null;
  public static HTable table = null;
  
  static {
    hbaseConfig = HBaseConfiguration.create();
  }
  
  public static void main(String[] args) throws IOException {
    try {
      table = new HTable(hbaseConfig, Constants.TABLE_NAME);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    Scan scan = new Scan();
    ResultScanner scanner = null;
    try {
      scanner = table.getScanner(scan);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    
    int duplicates = 0;
    long duplicateSize = 0;
    
    /*MessageDigest md = null;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }*/
    
    
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      //if(rr.raw().length == 0)
        //continue;
      //byte[] key = rr.getRow();
      //Get get = new Get(key);
      //Result rs = table.get(get);
      for(int i=1;i<rr.raw().length;i++){
        if(Arrays.equals(ResponseRecord.getBodyByte(rr.raw()[i].getValue()), ResponseRecord.getBodyByte(rr.raw()[i - 1].getValue()))){
          duplicates++;
          duplicateSize += rr.raw()[i].getValue().length;
        }
          
      }
    }
    
    System.out.println("Number of Duplicates: " + duplicates);
    System.out.println("Total Duplicate size: " + duplicateSize);
  }
}
