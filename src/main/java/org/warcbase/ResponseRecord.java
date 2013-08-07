package org.warcbase;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hbase.util.Bytes;


public class ResponseRecord {
	private byte[] data;
	
	public ResponseRecord(byte[] d){
		this.data = d;
	}
	
	public String getUTF8(){
		 String retString=null;
		    try {
		      retString = new String(data, "UTF-8");
		      if(retString.length() != data.length)
		    	  retString = new String(data, "ASCII");
		      if(retString.length() != data.length)
		    	  retString = new String(data, "US-ASCII");
		      if(retString.length() != data.length)
		    	  retString = new String(data, "ISO-8859-1");
		      if(retString.length() != data.length)
		    	  retString = new String(data);
		      if(retString.length() != data.length)
		    	  System.out.println("still not equal.");
		    } catch (UnsupportedEncodingException ex) {
		      retString=new String(data);
		    }
		    return retString;
	}
	
	public void setFromString(String stringData){
		this.data = Bytes.toBytes(stringData);
	}
	
	public byte[] getData(){
		return this.data;
	}
	
	public int length(){
		return data.length;
	}

	public static String getType(String record){
		String lines[] = record.split("\\r?\\n");
		for(String line: lines){
			if(line.startsWith("Content-Type")){
				return line.substring(14).split(";")[0];
			}
		}
		return "";
	}
	
	public static String getLength(String record){
		String lines[] = record.split("\\r?\\n");
		for(String line: lines){
			if(line.startsWith("Content-Length")){
				return line.substring(16).split(";")[0];
			}
		}
		return "";	
	}
	
	public static String getBodyText(String record){
		String body = record;
		while(!body.startsWith("\n"))
			body = body.substring(body.indexOf('\n')+1);
		body = body.substring(body.indexOf('\n')+1);
		return body;
	}
	
	public static String byteA2String(byte[] data){
		 String retString = null;
		    try {
		      retString = new String(data, "UTF-8");
		      if(retString.length() != data.length)
		    	  retString = new String(data, "ASCII");
		      if(retString.length() != data.length)
		    	  retString = new String(data, "US-ASCII");
		      //if(retString.length() != data.length)
		    	//  retString = new String(data, "ISO-8859-1");
		      if(retString.length() != data.length)
		    	  retString = new String(data);
		      if(retString.length() != data.length)
		    	  System.out.println("still not equal.");
		    } catch (UnsupportedEncodingException ex) {
		      retString=new String(data);
		    }
		    return retString;
	}
	
	public static byte[] getBodyByte(byte[] data){
		/*String body = new String(data);
		String lines[] = body.split("\\r?\\n");
		for(int i=0;i<lines.length;i++){
			d += (lines[i].length() + 1);
			System.out.println(lines[i]);
			if(lines[i].equals(""))
				break;
		}*/
		int d = 0;
		String body = byteA2String(data);
		if(!body.contains("\n"))
		  return data;
		//System.err.println(body);
		//if(true)
		//return data;
		//System.err.println("1");
		while(!body.startsWith("Connection") ){
			//System.err.println(body.substring(0, body.indexOf('\n')) + " " + body.indexOf('\n'));
			//if(body.indexOf('\n') == 1)
				//System.err.println("at 0: "+ body.charAt(0) + "\n".startsWith("\n") + "\n".indexOf('\n') + body.indexOf(' '));
			body = body.substring(body.indexOf('\n')+1);
		}
		body = body.substring(body.indexOf('\n')+1);
		while(body.charAt(0) == 'c' || body.charAt(0) == 'C' || body.charAt(0) == 'S' || body.charAt(0) == 'D')
			body = body.substring(body.indexOf('\n')+1);
		//System.err.println(2);
		
		body = body.substring(body.indexOf('\n')+1);
		return Arrays.copyOfRange(data, data.length - body.length(), data.length);
	}
	
	public static void main(String[] args) {
	  String test = "Salam \n chetory?";
	  System.out.println(test.contains("\n"));
	  if(true)
	    return;
		// TODO Auto-generated method stub
		File inputWarcFile = new File("/Users/milad/UMD/CL/US_Congress/archive/WARCs/ARCHIVEIT-3566-WEEKLY-23128-20130302043208784-00007-wbgrp-svc113.us.archive.org-6443.warc.gz");
		GZIPInputStream gzInputStream = null;
		try {
			System.out.println("Processing File: " + inputWarcFile.getName());
			gzInputStream = new GZIPInputStream(new FileInputStream(inputWarcFile));
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		DataInputStream inStream=new DataInputStream(gzInputStream);
	    
		// iterate through our stream
		WarcRecord thisWarcRecord = null;
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
					if(thisTargetURI.length() > 3 && thisTargetURI.substring(thisTargetURI.length() - 3, thisTargetURI.length()).equals("flv"))
						continue;
					if(thisTargetURI.length() > 3 && thisTargetURI.substring(thisTargetURI.length() - 3, thisTargetURI.length()).equals("mov"))
						continue;
					if(thisTargetURI.length() > 3 && thisTargetURI.substring(thisTargetURI.length() - 3, thisTargetURI.length()).equals("wmv"))
						continue;
					if(thisTargetURI.length() > 3 && thisTargetURI.substring(thisTargetURI.length() - 3, thisTargetURI.length()).equals("mp4"))
						continue;
					byte[] data = thisWarcRecord.getByteContent();
					if(getType(new String(data)).equals("image/jpeg")){
						System.out.println(thisTargetURI);
					}
					if(getType(new String(data)).startsWith("image")){
						//getBodyByte(data);
						//System.out.println(new String(getBodyByte(data)));
						System.out.println(Integer.parseInt(getLength(new String(data))));
						//System.out.println(new String(data));
						FileOutputStream output = null;
			    		try {
			    			output = new FileOutputStream(new File("mrw-on-ftn.jpg"));
			    		} catch (FileNotFoundException e) {
			    			// TODO Auto-generated catch block
			    			e.printStackTrace();
			    		}
			    		try {
			    			IOUtils.write(getBodyByte(data), output);
			    		} catch (IOException e) {
			    			// TODO Auto-generated catch block
			    			e.printStackTrace();
			    		}
			    		output.close();
						return;
					}
				}
			}
			
		} catch (IOException e2) {
			e2.printStackTrace();
		}
		catch(Exception e){
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
