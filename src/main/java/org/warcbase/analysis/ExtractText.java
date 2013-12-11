package org.warcbase.analysis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.warcbase.data.Util;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import tl.lin.data.SortableEntries.Order;
import tl.lin.data.fd.Object2IntFrequencyDistribution;
import tl.lin.data.fd.Object2IntFrequencyDistributionEntry;
import tl.lin.data.pair.PairOfObjectInt;

public class ExtractText {
  private static final Logger LOG = Logger.getLogger(ExtractText.class);
  private static final String NAME_OPTION = "name";
  private static final String DIR_OPTION = "dir";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException, NoSuchAlgorithmException {
    Options options = new Options();
    options.addOption(OptionBuilder.withArgName("name").hasArg()
        .withDescription("name of the archive").create(NAME_OPTION));
    options.addOption(OptionBuilder.withArgName("dir").hasArg()
        .withDescription("WARC files location").create(DIR_OPTION));
    
    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(DIR_OPTION) || !cmdline.hasOption(NAME_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(CountRowTypes.class.getCanonicalName(), options);
      System.exit(-1);
    }
    String name = cmdline.getOptionValue(NAME_OPTION);
    String path = cmdline.getOptionValue(DIR_OPTION);
    
    List<String> originalIds = new ArrayList<String>(100);
    List<String> ids = new ArrayList<String>(100);
    BufferedReader reader = new BufferedReader(new FileReader("ids.txt"));
    String line = null;
    while ((line = reader.readLine()) != null) {
      originalIds.add(line);
    }
    reader.close();
    
    for (String s : originalIds) {
      if (!ids.contains(s) && !s.equals("go")) {
        ids.add(s);
      }
    }

    /*for(int i=0;i<ids.size();i++){
      File folder = new File(path + ids.get(i));
      if(!folder.exists()){
        folder.mkdirs();
      }
    }*/
    Object2IntFrequencyDistribution<String>[] idsUrl = new Object2IntFrequencyDistributionEntry[ids.size()];
    for(int i=0;i<ids.size();i++) {
      idsUrl[i] = new Object2IntFrequencyDistributionEntry<String>();
    }
    
    HTablePool pool = new HTablePool();
    HTableInterface table = pool.getTable(name);
    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());
    ResultScanner scanner = null;
    scanner = table.getScanner(scan);

    String type = "";
    String content = "";
    for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
      byte[] key = rr.getRow();
      
      String id = "";
      int idInd = 0;
      String keyStr = Bytes.toString(key);
      String uri = Util.reverseBacUri(keyStr);
      boolean ambiguous = false;
      for(int i=0;i<ids.size();i++){
        if(keyStr.substring(0, Math.min(40, keyStr.length())).contains(ids.get(i))){
          if(!id.equals("")){
            //System.out.println(id + " " + ids.get(i));
            ambiguous = true;
          }
          id = ids.get(i);
          idInd = i;
        }
      }
      
      if(id.equals("") || ambiguous){
        continue;
      }
      
      String[] splits = keyStr.split("\\/");
      String base = null;
      if(splits[0] != null) {
        base = splits[0];
      }
      else {
        System.err.println(splits.length + " " + keyStr);
      }
      if(splits.length > 1) {
        base = base + "/" + splits[1];
      }
      idsUrl[idInd].increment(base);
      if(true) continue;
      //String domain = Bytes.toString(key);
      //domain = Util.getDomain(domain);
      //domain = Util.reverseBacHostnamek(domain);
      //String filePath = path + domain;
      String filePath = path + id;
      //File folder = new File(filePath);
      /*if(!folder.exists()){
        folder.mkdirs();
        //folder.createNewFile();
      }*/
      
      Get get = new Get(key);
      Result rs = table.get(get);
        
      for (int i=0;i<rs.raw().length;i++){
        if((new String(rs.raw()[i].getFamily(), "UTF8").equals("type"))){
          type = Bytes.toString(rs.raw()[i].getValue());
        }
      
      }
      if (!(type.contains("html"))){
        continue;
      }
      for (int i=0;i<rs.raw().length;i++){
        if (!(new String(rs.raw()[i].getFamily(), "UTF8").equals("content"))){
          continue;
        }
        content = Bytes.toString(rs.raw()[i].getValue());
        //System.out.println(content);
        String cleaned = Jsoup.parse(content).text().replaceAll("[\\r\\n]+", " ");
        //System.out.println(cleaned);
        //return;
        //FileWriter out = new FileWriter(filePath + "/" + Base64.encodeBase64URLSafeString(rs.raw()[i].getQualifier()) + Base64.encodeBase64URLSafeString(key) + ".txt", true);
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        //String password = new String(Hex.encodeHex(md.digest()),
          //  CharSet.forName("UTF-8"));
        FileWriter out = new FileWriter(filePath + "/" + Bytes.toString(rs.raw()[i].getQualifier()) + DigestUtils.sha1Hex(key) + ".txt", true);
        out.write(cleaned);
        out.close();
      }
    }
    
    pool.close();
    
    for(int i=0;i<ids.size();i++){
      System.out.print(ids.get(i));
      for (PairOfObjectInt<String> entry : idsUrl[i].getEntries(Order.ByRightElementDescending)) {
        System.out.print(", " + entry.getLeftElement());
      }
      System.out.println();
    }
  }
}
