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
import static org.junit.Assert.assertTrue;

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
import org.archive.io.warc.WARCRecord;
import org.junit.Test;
import org.warcbase.io.WarcRecordWritable;

import com.google.common.io.Resources;

public class WacWarcInputFormatTest {
  @Test
  public void testInputFormat() throws Exception {
    String[] urls = new String[] {
        null,
        "dns:www.archive.org",
        "http://www.archive.org/robots.txt",
        "http://www.archive.org/robots.txt",
        "http://www.archive.org/robots.txt",
        "http://www.archive.org/",
        "http://www.archive.org/",
        "http://www.archive.org/",
        "http://www.archive.org/index.php",
        "http://www.archive.org/index.php"};

    String[] types = new String[] {
        "warcinfo",
        "response",
        "response",
        "request",
        "metadata",
        "response",
        "request",
        "metadata",
        "response",
        "request"};

    String arcFile = Resources.getResource("warc/example.warc.gz").getPath();

    Configuration conf = new Configuration(false);
    conf.set("fs.defaultFS", "file:///");

    File testFile = new File(arcFile);
    Path path = new Path(testFile.getAbsoluteFile().toURI());
    FileSplit split = new FileSplit(path, 0, testFile.length(), null);

    InputFormat<LongWritable, WarcRecordWritable> inputFormat =
        ReflectionUtils.newInstance(WacWarcInputFormat.class, conf);
    TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    RecordReader<LongWritable, WarcRecordWritable> reader =
        inputFormat.createRecordReader(split, context);

    reader.initialize(split, context);

    assertTrue(urls.length == types.length);

    int cnt = 0;
    int responseCnt = 0;
    while (reader.nextKeyValue()) {
      WARCRecord record = reader.getCurrentValue().getRecord();

      if (cnt < urls.length) {
        assertEquals(urls[cnt], record.getHeader().getUrl());
        assertEquals(types[cnt], record.getHeader().getHeaderValue("WARC-Type"));
      }

      if (record.getHeader().getHeaderValue("WARC-Type").equals("response")) {
        responseCnt++;
      }

      cnt++;
    }
    assertEquals(822, cnt);
    assertEquals(299, responseCnt);
  }
}
