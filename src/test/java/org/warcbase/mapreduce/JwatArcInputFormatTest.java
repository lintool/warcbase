package org.warcbase.mapreduce;

import static org.junit.Assert.assertEquals;

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
import org.jwat.arc.ArcRecordBase;

import com.google.common.io.Resources;

public class JwatArcInputFormatTest {
  @Test
  public void testInputFormat() throws Exception {
    String[] urls = new String[] {
        "filedesc://IAH-20080430204825-00000-blackbook.arc",
        "dns:www.archive.org",
        "http://www.archive.org/robots.txt",
        "http://www.archive.org/",
        "http://www.archive.org/index.php" };
        
    String arcFile = Resources.getResource("arc/example.arc.gz").getPath();

    Configuration conf = new Configuration(false);
    conf.set("fs.defaultFS", "file:///");

    File testFile = new File(arcFile);
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat<LongWritable, ArcRecordBase> inputFormat =
        ReflectionUtils.newInstance(JwatArcInputFormat.class, conf);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    RecordReader<LongWritable, ArcRecordBase> reader =
        inputFormat.createRecordReader(split, context);

    reader.initialize(split, context);

    int cnt = 0;
    while (reader.nextKeyValue()) {
      ArcRecordBase record = reader.getCurrentValue();
      if (cnt < urls.length) {
        assertEquals(urls[cnt], record.getUrl().toString());
      }
      cnt++;
    }
    assertEquals(300, cnt);
  }
}
