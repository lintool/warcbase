package org.warcbase;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


public class LoadWARC {
	
  private static final String DIR_OPTION = "dir";
  private static final String START_OPTION = "start";
  
    public static Configuration hbaseConfig = null;
    public static HTable table = null;
    
    static {
    	hbaseConfig = HBaseConfiguration.create();
    }


    private static HashMap<String, String> get_headers(String doc) {
    	HashMap<String, String> hdr = new HashMap(20);
    	try {
    		BufferedReader in = new BufferedReader(new StringReader(doc));
    		int nl = 0;
    		String line = null;
    		while ((line = in.readLine()) != null) {
    			if (line.length() == 0)
    				nl++;
    			if (nl == 2)
    				break;
    			int i = line.indexOf(':');
    			if (i == -1)
    				continue;
    			try {
    				hdr.put(line.substring(0, i), line.substring(i+2));
    			} catch (Exception e) {}
    		}
    		StringBuilder buf = new StringBuilder();
    		while ((line = in.readLine()) != null) {
    			buf.append(line).append('\n');
    		}
    		hdr.put("document", buf.toString());
    	} catch (IOException e) {}
    	return hdr;
    }
    
    public static void creatTable(){
        HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(hbaseConfig);
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        try {
			if (admin.tableExists(Constants.TABLE_NAME)) {
			    System.out.println("table already exists!");
			} else {
			    HTableDescriptor tableDesc = new HTableDescriptor(Constants.TABLE_NAME);
			    for (int i = 0; i < Constants.FAMILYS.length; i++) {
			        tableDesc.addFamily(new HColumnDescriptor(Constants.FAMILYS[i]));
			    }
			    admin.createTable(tableDesc);
			    System.out.println("create table " + Constants.TABLE_NAME + " ok.");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public static void addRecord(String key, String date, byte[] data){
    	if(table == null){
    		try {
				table = new HTable(hbaseConfig, Constants.TABLE_NAME);
			} catch (IOException e) {
				System.out.println("addRecord exception: error in setting table");
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	Put put = new Put(Bytes.toBytes(key));
    	put.add(Bytes.toBytes(Constants.FAMILYS[0]), Bytes.toBytes(date), data);
    	//put.add(Bytes.toBytes(Constants.FAMILYS[1]), Bytes.toBytes(""), Bytes.toBytes(content));
    	try {
			table.put(put);
		} catch (IOException e) {
			System.out.println("addRecord exception: Couldn't insert key: " + key);
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	//System.out.println("insert recored " + key + " to table "
          //      + Constants.TABLE_NAME + " ok.");
    }
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
	  Options options = new Options();
	  options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("WARC files location").create(DIR_OPTION));
	  options.addOption(OptionBuilder.withArgName("start").hasArg()
        .withDescription("Start from WARC file").create(START_OPTION));
	  
	  CommandLine cmdline = null;
	  CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }
    
    if (!cmdline.hasOption(DIR_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(LoadWARC.class.getCanonicalName(), options);
      System.exit(-1);
    }
    String path = cmdline.getOptionValue(DIR_OPTION);
    //System.out.println(path);
		//if(true)
			//return;
		
    //Create hbase table
		creatTable();
		if(table == null){
    		try {
				table = new HTable(hbaseConfig, Constants.TABLE_NAME);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}

		Field maxKeyValueSizeField = null;
		try {
			maxKeyValueSizeField = HTable.class.getDeclaredField("maxKeyValueSize");
		} catch (SecurityException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NoSuchFieldException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		maxKeyValueSizeField.setAccessible(true);
		try {
			maxKeyValueSizeField.set(table, 0);
		} catch (IllegalArgumentException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			System.out.println(maxKeyValueSizeField.get(table));
		} catch (IllegalArgumentException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		File inputWarcFolder = new File(path);
		//File inputWarcFolder = new File("/Users/milad/UMD/CL/US_Congress/archive/partner.archive-it.org/cgi-bin/getarcs.pl");
		//File inputWarcFolder = new File("/scratch0/milad/partner.archive-it.org/cgi-bin/getarcs.pl");
		//String inputWarcFile = "/Users/milad/workspace/java/Senate/ARCHIVEIT-3395-NONE-KQSJEZ-20121203173122-00012-wbgrp-crawl068.us.archive.org-6680.warc.gz";
		//File inputWarcFile = new File("/Users/milad/UMD/CL/US_Congress/archive/partner.archive-it.org/cgi-bin/getarcs.pl/ARCHIVEIT-3566-DAILY-13015-20130222204213709-00000-wbgrp-crawl062.us.archive.org-6443.warc.gz");
		//for(File inputWarcFile: inputWarcFolder.listFiles()){
		System.out.println(inputWarcFolder.listFiles().length + " Files in total.");
		int i = 0;
		if(cmdline.hasOption(START_OPTION)) {
      i = Integer.parseInt(cmdline.getOptionValue(START_OPTION));
    }
		
		for(;i<inputWarcFolder.listFiles().length;i++){
			File inputWarcFile = inputWarcFolder.listFiles()[i];
			if(inputWarcFile.getName().charAt(0) == '.')
				continue;
			GZIPInputStream gzInputStream = null;
			try {
				System.out.println("Processing File: " + i + " = " + inputWarcFile.getName());
				gzInputStream = new GZIPInputStream(new FileInputStream(inputWarcFile));
			} catch (FileNotFoundException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			// cast to a data input stream
			DataInputStream inStream=new DataInputStream(gzInputStream);
	    	    
			// iterate through our stream
			WarcRecord thisWarcRecord;
			HashMap<String, String> parse = null;
			String thisTargetURI = null;
			try {
				while ((thisWarcRecord=WarcRecord.readNextWarcRecord(inStream))!=null) {
					// see if it's a response record
					if (thisWarcRecord.getHeaderRecordType().equals("response")) {
						// it is - create a WarcHTML record
						WarcHTMLResponseRecord htmlRecord=new WarcHTMLResponseRecord(thisWarcRecord);
						// get our TREC ID and target URI
						thisTargetURI=htmlRecord.getTargetURI();
						if(thisTargetURI.length() > 3 && thisTargetURI.substring(thisTargetURI.length() - 3, thisTargetURI.length()).equals("mp3"))
							continue;
						//if(thisTargetURI.length() > 3 && thisTargetURI.substring(thisTargetURI.length() - 3, thisTargetURI.length()).equals("flv"))
							//continue;
						if(thisTargetURI.length() > 3 && thisTargetURI.substring(thisTargetURI.length() - 3, thisTargetURI.length()).equals("mov"))
							continue;
						if(thisTargetURI.length() > 3 && thisTargetURI.substring(thisTargetURI.length() - 3, thisTargetURI.length()).equals("wmv"))
							continue;
						if(thisTargetURI.length() > 3 && thisTargetURI.substring(thisTargetURI.length() - 3, thisTargetURI.length()).equals("mp4"))
							continue;
						String content = thisWarcRecord.getContentUTF8();
						content = thisWarcRecord.toString();
						parse = get_headers(content);
						String key = Util.reverse_hostname(thisTargetURI);
						if(key == null)
							continue;
						//System.out.println(key);
						addRecord(key, parse.get("WARC-Date"), thisWarcRecord.getByteContent());//parse.get("document"));
					}
				}
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				System.out.println("exception2: " + thisTargetURI);
				System.out.println(Util.reverse_hostname(thisTargetURI));
				e2.printStackTrace();
			}
			catch(Exception e){
				System.out.println("exception: " + thisTargetURI);
				System.out.println(parse.get("document").length());
				//System.out.println(thisTargetURI.substring(thisTargetURI.length() - 3, thisTargetURI.length()).equals("mp3"));
				e.printStackTrace();
			}
	    
			try {
				inStream.close();
			} catch (IOException e2) {
				// TODO Auto-generated catch block
				e2.printStackTrace();
			}
			
			System.out.println("Added file: " + inputWarcFile.getName());
		}

		
	}

}
