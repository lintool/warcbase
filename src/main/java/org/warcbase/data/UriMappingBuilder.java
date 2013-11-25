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
import java.util.List;

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
	private static List readURL(String fileName) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(fileName));
		List url = new ArrayList<String>();
		String line;
		while ((line = br.readLine()) != null) {
			// This need to modify according to your input file
			if (line != "") {
				line = line.substring(4);
				url.add(line);
			}
		}
		br.close();
		return url;
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		String inputFileName = new String();
		String outputFileName = new String();
		if (args.length > 0) { // read file name from main arguments
			inputFileName = args[0];
			outputFileName = args[1];
		}
		List inputValues = null;
		try {
			inputValues = readURL(inputFileName); // read data
		} catch (IOException e) {
			e.printStackTrace();
		}
		// Be Careful about the file size
		long size = inputValues.size();
		List outputValues = new ArrayList<Long>(); // create the mapping id
													// array

		for (long i = 0; i < size; i++) {
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
				builder.add(Util.toIntsRef(scratchBytes, scratchInts),
						(Long) outputValues.get(i));
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
