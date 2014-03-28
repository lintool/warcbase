package org.warcbase.data;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.search.suggest.fst.FSTCompletion.Completion;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.FST.INPUT_TYPE;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.apache.lucene.util.fst.Util.MinResult;

public class TestFSTPrefix {

  public static void main(String[] args) throws IOException {
    // TODO Auto-generated method stub
    String inputValues[] = {"cat", "catch","cut", "doga","dogb","dogs"};
    long outputValues[] = {1,2,3,4,5,6};
    
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    Builder<Long> builder = new Builder<Long>(INPUT_TYPE.BYTE1, outputs);
    BytesRef scratchBytes = new BytesRef();
    IntsRef scratchInts = new IntsRef();
    for (int i = 0; i < inputValues.length; i++) {
      scratchBytes.copyChars(inputValues[i]);
      builder.add(Util.toIntsRef(scratchBytes, scratchInts), outputValues[i]);
    }
    FST<Long> fst = builder.finish();
    FSTPrefixSearch fstPrefix = new FSTPrefixSearch(fst);
    List<String> results = fstPrefix.prefixSearch("do");
    Iterator<String> iter = results.iterator();
    while(iter.hasNext()){
      System.out.println(iter.next());
    }
    System.out.println(results.size());
    Long[] idRange = fstPrefix.getIdRange(results);
    System.out.println(idRange[0]+" , "+idRange[1]);
  }
  


}
