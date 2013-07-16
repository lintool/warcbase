package org.warcbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.RowCounter;
import org.apache.hadoop.mapreduce.Job;

public class WarcbaseStatus {
  
  private static Configuration hbaseConfig = null;
  private static HTable table = null;
  
  static {
    hbaseConfig = HBaseConfiguration.create();
    try {
      table = new HTable(hbaseConfig, Constants.TABLE_NAME);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public int getNumOfPages(){
    try {
      Job job = new Job();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    //return job.getCounters().findCounter(RowCounter.RowCounterMapper.Counters.ROWS).getVal‌​ue();;
    return 0;
  }
  
  public int getTableSize(){
    return 0;
  }
  

}
