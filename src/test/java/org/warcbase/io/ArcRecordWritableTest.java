package org.warcbase.io;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;
import org.warcbase.mapreduce.WacArcInputFormat;

import com.google.common.io.Resources;

public class ArcRecordWritableTest {
  @Test
  public void testInputFormat() throws Exception {
    String arcFile = Resources.getResource("arc/example.arc.gz").getPath();

    Configuration conf = new Configuration(false);
    conf.set("fs.defaultFS", "file:///");

    File testFile = new File(arcFile);
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat<LongWritable, ArcRecordWritable> inputFormat = ReflectionUtils.newInstance(
        WacArcInputFormat.class, conf);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    RecordReader<LongWritable, ArcRecordWritable> reader = inputFormat.createRecordReader(split,
        context);

    reader.initialize(split, context);

    int cnt = 0;
    while (reader.nextKeyValue()) {
      ArcRecordWritable record = reader.getCurrentValue();
      cnt++;

      ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
      DataOutputStream dataOut = new DataOutputStream(bytesOut);

      record.write(dataOut);

      ArcRecordWritable reconstructed = new ArcRecordWritable();

      reconstructed.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

      assertEquals(record.getRecord().getMetaData().getUrl(),
          reconstructed.getRecord().getMetaData().getUrl());
    }

    assertEquals(300, cnt);
  }
}
