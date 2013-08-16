package org.warcbase;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class WarcRecordParser {
  private byte[] data;
  private String content;
  private String contentASCII;
  private String lines[];
  private byte[] bodyByte;
  private String type;
  
  public WarcRecordParser(byte[] data){
    this.data = data;
    try {
      this.content = new String(data, "UTF8");
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    this.lines = content.split("\\r?\\n");
    this.type = this.computeType();
    this.contentASCII = this.byteA2String();
    this.bodyByte = computeBodyByte();
  }
  
  
  private String computeType(){
    for(String line: lines){
      if(line.startsWith("Content-Type") || line.startsWith("Content-type") || line.toLowerCase().startsWith("content-type")){
        return line.substring(14).split(";")[0];
      }
    }
    return "";
  }
  
	public String getType(){
		return type;
	}
	
	public String getLength(){
    for(String line: lines){
      if(line.startsWith("Content-Length")){
        return line.substring(16).split(";")[0];
      }
    }
    return "";  
  }
	
	public String getBodyText(){
    String body = content;
    while(!body.startsWith("\n"))
      body = body.substring(body.indexOf('\n')+1);
    body = body.substring(body.indexOf('\n')+1);
    return body;
  }
	
	public String getBody(){
		String body = content;
		while(!body.startsWith("\n"))
			body = body.substring(body.indexOf('\n')+1);
		body = body.substring(body.indexOf('\n')+1);
		return body;
	}
	
	private String byteA2String(){
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
	
	private byte[] computeBodyByte(){
    int d = 0;
    String body = contentASCII;
    if(!body.contains("\n"))
      return data;
    /*Boolean hadConnection = false;
    while(body.contains("Connection") && !body.startsWith("Connection") && body.contains("\n")){
      //System.out.println(body.substring(0, body.indexOf('\n')));
      body = body.substring(body.indexOf('\n')+1);
      hadConnection = true;
    }
    System.out.println("hadConnection? = " + hadConnection);
    while(!hadConnection && body.contains("Via") && !body.startsWith("Via") && body.contains("\n")){
      //System.out.println(body.substring(0, body.indexOf('\n')));
      body = body.substring(body.indexOf('\n')+1);
    }
    //System.out.println("After Via");
    String lines[] = body.split("\\r?\\n");
    for(int i=0;i<10;i++)
      System.out.println(lines[i]);
    body = body.substring(body.indexOf('\n')+1);
    while((body.charAt(0) == 'c' || body.charAt(0) == 'C' || body.charAt(0) == 'S' || body.charAt(0) == 'D')  && body.contains("\n"))
      body = body.substring(body.indexOf('\n')+1);
    */
    while(body.contains("\n") && !(body.startsWith("\n") || body.startsWith(" ") || body.indexOf('\n') < 3) && body.contains("\n")){
      //System.out.println(body.substring(0, body.indexOf('\n')));
      body = body.substring(body.indexOf('\n')+1);
    }
    
    body = body.substring(body.indexOf('\n')+1);
    return Arrays.copyOfRange(data, data.length - body.length(), data.length);
  }
	
	public byte[] getBodyByte(){
	  return this.bodyByte;
	}
	
	public static void main(String[] args) {
		String test = new String("Content-Type: text/html; charset=UTF-8");
		System.out.println(test.substring(14).split(";")[0]);
	}
	
	/*public static void main(String[] args) {
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
}*/
}
