package org.warcbase.data;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.Util;

public class FSTPrefixSearch {
  
  private FST<Long> fst;
  
  public FSTPrefixSearch(FST<Long> fst){
    this.fst = fst;
  }
  
  public List<String> prefixSearch(String prefix) throws IOException{
    
    // descend to the arc of the prefix string
    Arc<Long> arc = fst.getFirstArc(new Arc<Long>());
    BytesReader fstReader = fst.getBytesReader();
    BytesRef bref = new BytesRef(prefix);    
    for(int i=0; i<bref.length; i++){
      fst.findTargetArc(bref.bytes[i+bref.offset] & 0xFF, arc, arc, fstReader);
    }
    
    // collect all substrings started from the arc of prefix string.
    List<BytesRef> result = new ArrayList<BytesRef>();
    BytesRef newPrefixBref = new BytesRef(prefix.substring(0, prefix.length()-1));
    collect(result, fstReader, newPrefixBref, arc);
    
    // convert BytesRef results to String results
    List<String> strResults = new ArrayList<String>();
    Iterator<BytesRef> iter = result.iterator();
    while(iter.hasNext()){
      strResults.add(iter.next().utf8ToString());
    }
    
    return strResults;
  }
  
  public Long[] getIdRange(List<String> results){
    Long startId=null, endId=null;
    String firstRes = results.get(0);
    String lastRes = results.get(results.size()-1);
    try {
      startId = Util.get(fst, new BytesRef(firstRes));
      endId = Util.get(fst, new BytesRef(lastRes));
    } catch (IOException e) {
      e.printStackTrace();
    }
    Long[] idrange = {startId, endId};
    return idrange;
  }
  
  private boolean collect(List<BytesRef> res, BytesReader fstReader, 
      BytesRef output, Arc<Long> arc) throws IOException {
    if (output.length == output.bytes.length) {
      output.bytes = ArrayUtil.grow(output.bytes);
    }
    assert output.offset == 0;
    output.bytes[output.length++] = (byte) arc.label;
    
    fst.readFirstTargetArc(arc, arc, fstReader);
    while (true) {
      if (arc.label == FST.END_LABEL) {
        res.add(new BytesRef().deepCopyOf(output));
        //if (res.size() >= num) return true;
      } else {
        int save = output.length;
        if (collect(res, fstReader, output, new Arc<Long>().copyFrom(arc))) {
          return true;
        }
        output.length = save;
      }
      
      if (arc.isLast()) {
        break;
      }
      fst.readNextArc(arc, fstReader);
    }
    return false;
  }
}
