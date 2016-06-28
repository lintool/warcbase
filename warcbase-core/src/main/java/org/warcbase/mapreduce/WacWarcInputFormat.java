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

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.archive.io.ArchiveRecord;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCReaderFactory.CompressedWARCReader;
import org.archive.io.warc.WARCRecord;
import org.warcbase.io.WarcRecordWritable;

public class WacWarcInputFormat extends FileInputFormat<LongWritable, WarcRecordWritable> {
  @Override
  public RecordReader<LongWritable, WarcRecordWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new WarcRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  public class WarcRecordReader extends RecordReader<LongWritable, WarcRecordWritable> {
    private WARCReader reader;
    private long start;
    private long pos;
    private long end;
    private LongWritable key = null;
    private WarcRecordWritable value = null;
    private Seekable filePosition;
    private Iterator<ArchiveRecord> iter;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
      FileSplit split = (FileSplit) genericSplit;
      Configuration job = context.getConfiguration();
      start = split.getStart();
      end = start + split.getLength();
      final Path file = split.getPath();

      FileSystem fs = file.getFileSystem(job);
      FSDataInputStream fileIn = fs.open(split.getPath());

      reader = (WARCReader) WARCReaderFactory.get(split.getPath().toString(),
          new BufferedInputStream(fileIn), true);

      iter = reader.iterator();
      //reader = (ARCReader) ARCReaderFactory.get(split.getPath().toString(), fileIn, true);

      this.pos = start;
    }

    private boolean isCompressedInput() {
      return reader instanceof CompressedWARCReader;
    }

    private long getFilePosition() throws IOException {
      long retVal;
      if (isCompressedInput() && null != filePosition) {
        retVal = filePosition.getPos();
      } else {
        retVal = pos;
      }
      return retVal;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      if (!iter.hasNext()) {
        return false;
      }

      if (key == null) {
        key = new LongWritable();
      }
      key.set(pos);

      WARCRecord record = (WARCRecord) iter.next();
      if (record == null) {
        return false;
      }

      if (value == null) {
        value = new WarcRecordWritable();
      }
      value.setRecord(record);

      return true;
    }

    @Override
    public LongWritable getCurrentKey() {
      return key;
    }

    @Override
    public WarcRecordWritable getCurrentValue() {
      return value;
    }

    @Override
    public float getProgress() throws IOException {
      if (start == end) {
        return 0.0f;
      } else {
        return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
      }
    }

    @Override
    public synchronized void close() throws IOException {
      reader.close();
    }
  }
}
