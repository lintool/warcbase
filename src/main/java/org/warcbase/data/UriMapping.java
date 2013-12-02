import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

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
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util.MinResult;
import org.apache.lucene.util.fst.Util;

public class UriMapping {
	private FST<Long> fst;

	public UriMapping(){}
	
	public UriMapping(String outputFileName) {
		PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
		File outputFile = new File(outputFileName);
		try {
			this.fst = FST.read(outputFile, outputs);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Build FST Failed");
			e.printStackTrace();
		}
	}
	
	public void loadMapping(String outputFileName){
		UriMapping tmp = new UriMapping(outputFileName);
		this.fst = tmp.fst;
	}
	
	public FST<Long> getFst(){
		return fst;
	}
	public int getID(String url) {
		Long id = null;
		try {
			id = Util.get(fst, new BytesRef(url));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("url may not exist");
			e.printStackTrace();
		}
		if(id == null){ //url don't exist
			return -1;
		}
		return id.intValue();
	}

	public String getUrl(int id) {
		BytesRef scratchBytes = new BytesRef();
		IntsRef key = null;
		try {
			key = Util.getByOutput(fst, id);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("id may not exist");
			e.printStackTrace();
		}
		return Util.toBytesRef(key, scratchBytes).utf8ToString();

	}
}