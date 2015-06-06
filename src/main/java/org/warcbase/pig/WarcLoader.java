package org.warcbase.pig;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCRecord;
import org.archive.util.ArchiveUtils;
import org.warcbase.data.WarcRecordUtils;
import org.warcbase.io.WarcRecordWritable;
import org.warcbase.mapreduce.WacWarcInputFormat;

import com.google.common.collect.Lists;

public class WarcLoader extends FileInputLoadFunc {
  private static final Logger LOG = Logger.getLogger(WarcLoader.class);

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();
  private static final DateFormat ISO8601 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX");

  private RecordReader<LongWritable, WarcRecordWritable> in;

  public WarcLoader() {
  }

  @Override
  public WacWarcInputFormat getInputFormat() throws IOException {
    return new WacWarcInputFormat();
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      WARCRecord record;
      ArchiveRecordHeader header;

      // We're going to continue reading WARC records from the underlying input format
      // until we reach a "response" record.
      while (true) {
        if (!in.nextKeyValue()) {
          return null;
        }

        record = (WARCRecord) in.getCurrentValue().getRecord();
        header = record.getHeader();

        if (header.getHeaderValue("WARC-Type").equals("response")) {
          break;
        }
      }

      String url = header.getUrl();
      byte[] content = null;

      try {
        content = WarcRecordUtils.getContent(record);
      } catch (OutOfMemoryError e) {
        // When we get a corrupt record, this will happen...
        // Try to recover and move on...
        LOG.error("Encountered OutOfMemoryError ingesting " + url);
        LOG.error("Attempting to continue...");
      }

      Date d = null;
      try {
        d = ISO8601.parse(header.getDate());
      } catch (ParseException e) {
        LOG.error("Encountered ParseException ingesting " + url);
      }

      String date = ArchiveUtils.get14DigitDate(d);
      String type = WarcRecordUtils.getWarcResponseMimeType(content);
      
      List<Object> protoTuple = Lists.newArrayList();
      protoTuple.add(url);
      protoTuple.add(date);
      protoTuple.add(type);
      protoTuple.add(new DataByteArray(content));

      return TUPLE_FACTORY.newTupleNoCopy(protoTuple);
    } catch (InterruptedException e) {
      int errCode = 6018;
      String errMsg = "Error while reading input";
      throw new ExecException(errMsg, errCode, PigException.REMOTE_ENVIRONMENT, e);
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Override
  public void prepareToRead(RecordReader reader, PigSplit split) {
    in = reader;
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    FileInputFormat.setInputPaths(job, location);
  }
}
