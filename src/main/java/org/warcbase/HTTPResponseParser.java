package org.warcbase;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;


public class HTTPResponseParser {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		File input = new File("response.txt");
		FileInputStream fin = null;
		BufferedInputStream bin = null;
		try {
			fin = new FileInputStream(input);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		bin = new BufferedInputStream(fin);
		byte[] contents = new byte[1024];
		int bytesRead=0;
        StringBuilder strFileContents = new StringBuilder();
        try {
			while( (bytesRead = bin.read(contents)) != -1){
			    
			    strFileContents.append(new String(contents, 0, bytesRead));
			    //System.out.print(strFileContents);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
        //System.out.println(strFileContents.toString());
        System.out.println(warcRecordParser.getBody(strFileContents.toString()));
        
	}

}
