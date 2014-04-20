package org.warcbase.data;

import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.warcbase.ingest.IngestFiles;

import com.csvreader.CsvReader;


public class ExtractSiteLinks {
  
  public class PrefixNode{
    int id;
    String url;
    Long startPos;
    Long endPos;
    
    public PrefixNode(int id, String url, Long startPos, Long endPos){
      this.id = id;
      this.url = url;
      this.startPos = startPos;
      this.endPos = endPos;
    }
    
    public int getId(){ return id; }
    public String getUrl(){ return url; }
    public Long getStartPos(){ return startPos; }
    public Long getEndPos(){ return endPos; }
  }
  
  static final String FST = "fstfile";
  static final String PREFIX = "prefixfile";
  static final String LINKS = "linkdir";
  static final String OUTPUT = "output";
  
  static UriMapping map;
  static ArrayList<PrefixNode> prefixes;
  static Int2ObjectAVLTreeMap<IntArrayList> links;
  static int [][] prefixLinks;
  
  private static void loadPrefix(String prefixFile) throws IOException{
    ExtractSiteLinks instance = new ExtractSiteLinks();
    final Comparator<PrefixNode> comparator = new Comparator<PrefixNode>(){
      @Override
      public int compare(PrefixNode n1, PrefixNode n2){
        if(n1.startPos > n2.startPos){
          return 1;
        }else if( n1.startPos == n2.startPos ){
          return 0;
        }else{
          return -1;
        }
      }
    };
    prefixes = new ArrayList<PrefixNode>();
    CsvReader reader = new CsvReader(prefixFile);
    reader.readHeaders();
    String line;
    while(reader.readRecord()){
      int id = Integer.valueOf(reader.get("id"));
      String url = reader.get("url");
      List<String> results = map.prefixSearch(url);
      Long[] boundary = map.getIdRange(results);
      PrefixNode node = instance.new PrefixNode(id, url, boundary[0], boundary[1]);
      prefixes.add(node);
    }
    Collections.sort(prefixes,comparator);
    prefixLinks = new int[prefixes.size()+1][prefixes.size()+1];
    reader.close();
  }
  
  private static void loadLinks(String linkDir) throws IOException{
    links = new Int2ObjectAVLTreeMap<IntArrayList>();
    File dataDir  = new File(linkDir);
    File[] files = dataDir.listFiles();
    for(File file:files){
      if(!file.getName().startsWith("part"))
        continue;
      String contents = FileUtils.readFileToString(file);
      String[] lines = contents.split("\\n");
      for(String line: lines){
        // This need to modify according to your input file
        line = line.replace('[', ' ');
        line = line.replace(']', ' ');
        line = line.replace(',', ' ');
        if (!line.equals("")) { // non-empty string 
          line.trim();
          String[] nodes = line.split("\\s+");
          IntArrayList list = new IntArrayList();
          for(int i=1; i<nodes.length; i++){
            list.add(Integer.valueOf(nodes[i]));
          }
          links.put(Integer.valueOf(nodes[0]), list);
        }
      }
      System.out.println("Init links of "+file.getName());
    }
  }
  
  private static int getPrefixId(int id){
    int start = 0, end = prefixes.size()-1;
    int mid;
    while(start<=end){
      mid = (start+end)/2;
      if(prefixes.get(mid).getStartPos() <= id && prefixes.get(mid).getEndPos() >= id){
        return prefixes.get(mid).getId();
      }else if(prefixes.get(mid).getStartPos() > id){
        end = mid-1;
      }else{
        start = mid+1;
      }
    }
    return -1;
  }
  
  private static void saveSiteLinks(String output) throws IOException{
    BufferedWriter writer = new BufferedWriter(new FileWriter(output));
    writer.write("Source,Target,Weight\n");
    for(int src=1; src<=prefixes.size();src++){
      for(int dest=1; dest<=prefixes.size();dest++){
        if(prefixLinks[src][dest]!=0 && src!=dest){
          writer.write(src+","+dest+","+prefixLinks[src][dest]+"\n");
        }
      }
    }
    writer.close();
  }
  
  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("FST data file").create(FST));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("prefix file").create(PREFIX));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("links dir").create(LINKS));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("extracted site links").create(OUTPUT));
    
    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: "
          + exp.getMessage());
      System.exit(-1);
    }
    
    if (!cmdline.hasOption(FST) || !cmdline.hasOption(PREFIX)
        || !cmdline.hasOption(LINKS) || !cmdline.hasOption(OUTPUT) ) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(IngestFiles.class.getCanonicalName(), options);
      System.exit(-1);
    }
    
    String fstFile = cmdline.getOptionValue(FST);
    String prefixFile = cmdline.getOptionValue(PREFIX);
    String linkDir = cmdline.getOptionValue(LINKS);
    String output = cmdline.getOptionValue(OUTPUT);
    
    map = new UriMapping(fstFile);
    loadPrefix(prefixFile);
    loadLinks(linkDir);
    
    for(PrefixNode node: prefixes){
      for(Long iterId=node.getStartPos(); iterId <= node.getEndPos(); iterId++){
        IntArrayList iterLinks = links.get(iterId.intValue());
        if(iterLinks == null) continue;
        for(int iterLink: iterLinks){
          int dest_id = getPrefixId(iterLink);
          if(dest_id != -1){
            int src_id = node.getId();
            prefixLinks[src_id][dest_id] += 1;
          }
        }
      }
    }
    
    saveSiteLinks(output);
  }

}
