import java.io.*;
import java.net.URL;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Fetch {
	
	  /**
	   Write a byte array to the given file. 
	   Writing binary data is significantly simpler than reading it. 
	  */
	  public static void write(byte[] aInput, String aOutputFileName){
	    try {
	      OutputStream output = null;
	      try {
	        output = new BufferedOutputStream(new FileOutputStream(aOutputFileName));
	        output.write(aInput);
	      }
	      finally {
	        output.close();
	      }
	    }
	    catch(FileNotFoundException ex){
	    }
	    catch(IOException ex){
	    }
	  }
	  
	  public static boolean isPureAscii(byte bytearray []) {
		    CharsetDecoder d = Charset.forName("US-ASCII").newDecoder();
		    try {
		      CharBuffer r = d.decode(ByteBuffer.wrap(bytearray));
		      r.toString();
		    }
		    catch(CharacterCodingException e) {
		      return false;
		    }
		    return true;
		  }
	  
	  public static void main(String[] args) throws IOException {

    	if(args.length < 1){
    		System.err.println("Should have at least one argument.");
    		return;
    	}
    	String query = args[0];
    	/*String date = null;
    	if(args.length > 1){
    		date = args[1];
    	}*/
    	//System.out.println(query);
    	//if(true)
    		//return;

    Configuration config = HBaseConfiguration.create();
	HTable table = new HTable(LoadWARC.hbaseConfig, Constants.TABLE_NAME);

	//String query = "com.nytimes.topics/top/reference/timestopics/subjects/a/agriculture/urban_agriculture/index.html/Microsoft.XMLHTTP?offset=10&s=newest&query=ANTIPOVERTY+PROGRAMS&field=des&match=exact";
	//String query = "http://topics.nytimes.com/top/reference/timestopics/subjects/a/agriculture/urban_agriculture/index.html/2.1_120516.0?query=OAKLAND%20(CALIF)&field=geo&match=exact";
	//String query = "http://www.boxer.senate.gov/";
	//String query = "http://www.boxer.senate.gov/";
	query = Util.reverse_hostname(query);
	Get get = new Get(Bytes.toBytes(query));
    Result rs = table.get(get);
    if(rs.raw().length == 0){
    	System.out.println("Not found.");
    	return;
    }
    byte[] data = rs.raw()[0].getValue();
    String content = new String(rs.raw()[0].getValue());
    System.out.println(data.length);
    System.out.println(content);
    Fetch.write(rs.raw()[0].getValue(), "pic.jpg");
    System.out.println(Fetch.isPureAscii(rs.raw()[0].getValue()));
    FileOutputStream fileOut = new FileOutputStream("test.jpeg");
    DataOutputStream os = new DataOutputStream(fileOut);
    os.write(data);
    os.close();
    if(true)
    	return;
    while(content.charAt(0) != '<' && content.contains("\n"))
    	content = content.substring(content.indexOf('\n')+1);
	TextDocument2 t2 = new TextDocument2(null, null, null);
    System.out.println(t2.fixURLs(content, "http://www.boxer.senate.gov/", "2013-02-12T20:42:22Z"));
    if(true)
    	return;
    //ArrayList<String> dates = new ArrayList<String>(rs.raw().length);
    ArrayList<String> dates = new ArrayList<String>(10);
    for(int i=0;i<rs.raw().length;i++)
    	dates.add(new String(rs.raw()[i].getQualifier()));
    dates.add("2013-02-12T20:42:22Z");
    dates.add("2013-01-02T20:42:22Z");
    dates.add("2013-02-22T02:42:22Z");
    dates.add("2013-02-22T20:42:23Z");
    dates.add("2011-12-22T20:42:22Z");
    dates.add("2013-12-23T03:53:40Z");
    for(String date:dates){
    	System.out.println(date);
    }
    System.out.println("\nsorting...\n");
    Collections.sort(dates);
    for(String date:dates){
    	System.out.println(date);
    }
    //for(KeyValue kv : rs.raw()){
        //System.out.print(new String(kv.getRow()) + " " );
        //System.out.print(new String(kv.getFamily()) + ":" );
        //System.out.print(new String(kv.getQualifier()) + " " );
        //System.out.print(kv.getTimestamp() + " " );
    //    System.out.println(new String(kv.getValue()));
    //}
    }
}
