package org.warcbase.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.INPUT_TYPE;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

public class UrlMappingBuilder {
  private static final Logger LOG = Logger.getLogger(UrlMappingBuilder.class);

  private static void readUrlFromFile(File f, List<String> urls) throws IOException {
    String contents = FileUtils.readFileToString(f);
    String[] lines = contents.split("\\n");
    for (String line : lines) {
      if (!line.equals("")) {
        String url = line.split("\\s+")[0];
        urls.add(url);
      }
    }
    LOG.info("Read " + f + ", " + urls.size() + " URLs");
  }

  private static List<String> readUrlFromFolder(String folderName) throws IOException {
    File folder = new File(folderName);
    List<String> urls = new ArrayList<String>();
    if (folder.isDirectory()) {
      for (File file : folder.listFiles()) {
        readUrlFromFile(file, urls);
      }
    } else {
      readUrlFromFile(folder, urls);
    }

    LOG.info("Sorting URLs...");
    Collections.sort(urls); // sort URLs alphabetically
    LOG.info("Done sorting!");

    return urls;
  }

  public static void main(String[] args) throws IOException {
    String inputFileName = new String();
    String outputFileName = new String();
    if (args.length > 0) { // read file name from main arguments
      inputFileName = args[0];
      outputFileName = args[1];
    }
    List<String> inputValues = null;
    try {
      inputValues = readUrlFromFolder(inputFileName); // read data
    } catch (IOException e) {
      e.printStackTrace();
    }

    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    Builder<Long> builder = new Builder<Long>(INPUT_TYPE.BYTE1, outputs);
    BytesRef scratchBytes = new BytesRef();
    IntsRef scratchInts = new IntsRef();
    for (int i = 0; i < inputValues.size(); i++) {
      if (i % 100000 == 0) {
        LOG.info(i + " URLs processed.");
      }
      scratchBytes.copyChars((String) inputValues.get(i));
      try {
        builder.add(Util.toIntsRef(scratchBytes, scratchInts), (long) i);
      } catch (UnsupportedOperationException e) {
        System.out.println("Duplicate URL:" + inputValues.get(i));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    FST<Long> fst = builder.finish();

    // Save FST to file
    File outputFile = new File(outputFileName);
    fst.save(outputFile);
    LOG.info("Wrote output to " + outputFileName);
  }
}
