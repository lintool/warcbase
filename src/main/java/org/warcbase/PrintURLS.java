package org.warcbase;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class PrintURLS {
  private static final String DIR_OPTION = "dir";
  private static final String O_OPTION = "o";

  public static void main(String[] args) {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("WARC files location").create(DIR_OPTION));
    options.addOption(OptionBuilder.withArgName("o").hasArg()
        .withDescription("Output file name").create(O_OPTION));
    
    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }
    
    if (!cmdline.hasOption(DIR_OPTION) || !cmdline.hasOption(O_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(LoadWARC.class.getCanonicalName(), options);
      System.exit(-1);
    }
    
    String path = cmdline.getOptionValue(DIR_OPTION);
    String outFile = cmdline.getOptionValue(O_OPTION);
    
    PrintStream out = null;
    try {
      out = new PrintStream(new FileOutputStream(outFile));
    } catch (FileNotFoundException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
    
    File inputWarcFolder = new File(path);
    for(int i = 0;i<inputWarcFolder.listFiles().length;i++){
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
      
      DataInputStream inStream=new DataInputStream(gzInputStream);
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
            out.println(thisTargetURI);
          }
        }
      }catch (IOException e2) {
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
    out.close();
  }
}
