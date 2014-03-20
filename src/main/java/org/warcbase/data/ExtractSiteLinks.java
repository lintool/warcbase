package org.warcbase.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.fst.FSTCompletion;
import org.apache.lucene.search.suggest.fst.FSTCompletion.Completion;
import org.apache.lucene.search.suggest.fst.FSTCompletionLookup;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.fst.NoOutputs;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;

public class ExtractSiteLinks {

  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    NoOutputs outputs = NoOutputs.getSingleton();
    File outputFile = new File("map.txt");
    FST<Object> fst = FST.read(outputFile, outputs); // load fst
    FSTCompletion fstCompletion = new FSTCompletion(fst);
    String prefix = "http";
    List<Completion> results = fstCompletion.lookup(prefix, 100); //return top-100 match results
    for(Completion match: results){
      System.out.println(match.toString());
    }
  }

}
