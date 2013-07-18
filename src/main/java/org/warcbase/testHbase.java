package org.warcbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class testHbase {
	
    public static Configuration hbaseConfig = null;
    public static HTable table = null;
    
    static {
    	hbaseConfig = HBaseConfiguration.create();
    }

    public static void addRecord(String key, String date, String content){
    	if(table == null){
    		try {
				table = new HTable(hbaseConfig, Constants.TABLE_NAME);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	Put put = new Put(Bytes.toBytes(key));
    	//put.add(family, qualifier, ts, value)
    	//put.setAttribute("name", Bytes.toBytes(""));
    	//put.add(Bytes.toBytes(Constants.FAMILYS[0]), Bytes.toBytes(""), Bytes.toBytes(date));
    	put.add(Bytes.toBytes(Constants.FAMILYS[0]), Bytes.toBytes(date), Bytes.toBytes(content));
    	try {
			table.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	System.out.println("insert recored " + key + " to table "
                + Constants.TABLE_NAME + " ok.");
    }


	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		LoadWARC.creatTable();
		String testId = "2";
		addRecord(testId, "2012", "html1");
		addRecord(testId, "2013", "html2");
		addRecord(testId, "2011", "html3");
		Get get = new Get(Bytes.toBytes(testId));
		Result rs = null;
	    try {
			rs = table.get(get);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    System.out.println(rs.raw().length);
	    for(int i=0;i<rs.raw().length;i++){
	    	String content = new String(rs.raw()[i].getValue());
	    	String family = new String(rs.raw()[i].getFamily());
	    	String qualifier = new String(rs.raw()[i].getQualifier());
	    	System.out.println(family + " " + qualifier + " " + content);
	    }
	}

}
