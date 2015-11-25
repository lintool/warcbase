/*
 * Warcbase: an open-source platform for managing web archives
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.junit.Test;
import org.warcbase.io.ArcRecordWritable;

import com.google.common.io.Resources;

public class WacArcInputFormatTest {
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

    InputFormat<LongWritable, ArcRecordWritable> inputFormat =
        ReflectionUtils.newInstance(WacArcInputFormat.class, conf);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    RecordReader<LongWritable, ArcRecordWritable> reader =
        inputFormat.createRecordReader(split, context);

    reader.initialize(split, context);

    int cnt = 0;
    while (reader.nextKeyValue()) {
      ARCRecord record = reader.getCurrentValue().getRecord();
      ARCRecordMetaData metadata = record.getMetaData();

      if (cnt < urls.length) {
        assertEquals(urls[cnt], metadata.getUrl());
      }
      cnt++;
    }
    assertEquals(300, cnt);
  }
}
