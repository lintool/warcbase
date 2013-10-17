package org.warcbase.data;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

public class ArcParser {
  private byte[] content = null;
  private String date = null;
  private String url = null;
  
  public ArcParser(byte[] arcRecord){
    String arcRecordString = null;
    try {
      arcRecordString = new String(arcRecord, "UTF8");
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    String first = arcRecordString.substring(0, arcRecordString.indexOf('\n'));
    //System.out.println(first);
    String[] splited = first.split("\\s+");
    this.date = splited[2];
    this.url = splited[0];
    this.content = Arrays.copyOfRange(arcRecord, first.length(), arcRecord.length);
    //System.out.println(url + " " + date);
  }
}
