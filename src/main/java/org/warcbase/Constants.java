package org.warcbase;
//import iu.pti.hbaseapp.clueweb09.HTMLTextParser;

import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.lucene.analysis.Analyzer;
//import org.apache.lucene.analysis.standard.StandardAnalyzer;
//import org.apache.lucene.util.Version;


public class Constants {
	public static final String TABLE_NAME = "Web_Archive";
//	public static final String[] FAMILIES = {"File"};
	
	public static final String CF_COUNTS = "counts";
	
	public static final byte[] CF_COUNTS_BYTES = Bytes.toBytes(CF_COUNTS);
		
	// Specially for the ClueWeb09 data set:
	public static final String CLUEWEB09_DATA_TABLE_NAME = "clueWeb09DataTable";
	public static final String WORD_COUNT_TABLE_NAME = "WordCountTable";
	
	public static final byte[] CW09_DATA_TABLE_BYTES = Bytes.toBytes(CLUEWEB09_DATA_TABLE_NAME);
	public static final byte[] WORD_COUNT_TABLE_BYTES = Bytes.toBytes(WORD_COUNT_TABLE_NAME);
	
	public static final String CF_DETAILS = "details";
	
	public static final byte[] CF_DETAILS_BYTES = Bytes.toBytes(CF_DETAILS);
	
	public static final String QUALIFIER_URI = "URI";
	public static final String QUALIFIER_CONTENT = "content";

	public static final byte[] QUAL_CONTENT_BYTES = Bytes.toBytes(QUALIFIER_CONTENT);
	public static final byte[] QUAL_URI_BYTES = Bytes.toBytes(QUALIFIER_URI);

	//public static final Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_31);
	//public static final HTMLTextParser txtExtractor = new HTMLTextParser();
	
	// data types
	public enum DataType {INT, STRING, DOUBLE, UNKNOWN};
}