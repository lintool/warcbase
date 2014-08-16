package org.warcbase.pig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.FileInputLoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.jwat.common.HttpHeader;
import org.jwat.common.Payload;
import org.jwat.warc.WarcRecord;
import org.warcbase.mapreduce.JwatWarcInputFormat;

import com.google.common.collect.Lists;

public class WarcLoader extends FileInputLoadFunc {
  public static final int MAX_CONTENT_SIZE = 1024 * 1024;
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private RecordReader<LongWritable, WarcRecord> in;

  public WarcLoader() {
  }

  @Override
  public JwatWarcInputFormat getInputFormat() throws IOException {
    return new JwatWarcInputFormat();
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      if ( !in.nextKeyValue() ) {
        return null;
      }

      WarcRecord record = in.getCurrentValue();

      String url = record.header.warcTargetUriStr;
      HttpHeader httpHeader = record.getHttpHeader();
      Payload payload = record.getPayload();
      String type = "";
      String date = "";
      String content = "";

      if (payload != null) {
        InputStream payloadStream = null;

        if (httpHeader != null) {
          payloadStream = httpHeader.getPayloadInputStream();
          type = httpHeader.contentType;
        } else {
          payloadStream = payload.getInputStreamComplete();
        }

        date = record.header.warcDateStr;

        if ( type == null ) {
          type = "";
        }

        if (payloadStream.available() <= MAX_CONTENT_SIZE) {
          if (type.toLowerCase().contains("text")) {
            content = new String(IOUtils.toByteArray(payloadStream), Charset.forName("UTF-8"));
          }
        }
      }
      
      List<String> protoTuple = Lists.newArrayList();
      protoTuple.add(url);
      protoTuple.add(date);
      protoTuple.add(type);
      protoTuple.add(content);

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
