package org.warcbase.pig;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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
import org.archive.io.arc.ARCRecordMetaData;
import org.warcbase.io.ArcRecordWritable;
import org.warcbase.mapreduce.WacArcInputFormat;

import com.google.common.collect.Lists;

public class ArcLoader extends FileInputLoadFunc {
  private static final Logger LOG = Logger.getLogger(ArcLoader.class);

  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private RecordReader<LongWritable, ArcRecordWritable> in;

  public ArcLoader() {
  }

  @Override
  public WacArcInputFormat getInputFormat() throws IOException {
    return new WacArcInputFormat();
  }

  @Override
  public Tuple getNext() throws IOException {
    try {
      if ( !in.nextKeyValue() ) {
        return null;
      }

      ArcRecordWritable r = in.getCurrentValue();
      ARCRecordMetaData meta = r.getRecord().getMetaData();

      List<Object> protoTuple = Lists.newArrayList();
      protoTuple.add(meta.getUrl());
      protoTuple.add(meta.getDate());
      protoTuple.add(meta.getMimetype());

      try {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(bytesOut);
        r.write(dataOut);

        protoTuple.add(new DataByteArray(bytesOut.toByteArray()));
      } catch (OutOfMemoryError e) {
        // When we get a corrupt record, this will happen...
        // Try to recover and move on...
        LOG.error("Encountered OutOfMemoryError ingesting " + meta.getUrl());
        LOG.error("Attempting to continue...");
      }

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
