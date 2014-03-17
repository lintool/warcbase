package org.warcbase.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.UnicodeUtil;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.BytesRefFSTEnum;
import org.apache.lucene.util.fst.BytesRefFSTEnum.InputOutput;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.INPUT_TYPE;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util.MinResult;
import org.apache.lucene.util.fst.Util;

public class UriMappingBuilder {

  private static void readUrlFromFile(File f, List urls) throws IOException {
    String contents = FileUtils.readFileToString(f);
    String[] lines = contents.split("\\n");
    for (String line : lines) {
      // This need to modify according to your input file
      if (!line.equals("")) { // non-empty string
        String url = line.split("\\s+")[0];
        urls.add(url);
      }
    }
  }

  private static List readUrlFromFolder(String folderName) throws IOException {
    File folder = new File(folderName);
    List urls = new ArrayList<String>();
    if (folder.isDirectory()) {
      for (File file : folder.listFiles()) {
        readUrlFromFile(file, urls);
      }
    } else {
      readUrlFromFile(folder, urls);
    }
    Collections.sort(urls); // sort String according to url alphabetical order
    return urls;
  }

  public static void main(String[] args) throws IOException {
    String inputFileName = new String();
    String outputFileName = new String();
    if (args.length > 0) { // read file name from main arguments
      inputFileName = args[0];
      outputFileName = args[1];
    }
    List inputValues = null;
    try {
      // input strings must be sorted in Unicode order
      inputValues = readUrlFromFolder(inputFileName); // read data
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Be Careful about the file size
    long size = inputValues.size();
    List outputValues = new ArrayList<Long>(); // create the mapping id

    for (long i = 1; i <= size; i++) {
      outputValues.add(i);
    }

    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    Builder<Long> builder = new Builder<Long>(INPUT_TYPE.BYTE1, outputs);
    BytesRef scratchBytes = new BytesRef();
    IntsRef scratchInts = new IntsRef();
    for (int i = 0; i < size; i++) {
      scratchBytes.copyChars((String) inputValues.get(i));
      try {
        // Mapping!
        builder.add(Util.toIntsRef(scratchBytes, scratchInts), (Long) outputValues.get(i));
      } catch (UnsupportedOperationException e) {
        System.out.println("Duplicate Url:" + inputValues.get(i));
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    FST<Long> fst = builder.finish();

    // Save FST to file
    File outputFile = new File(outputFileName);
    fst.save(outputFile);
  }
}
