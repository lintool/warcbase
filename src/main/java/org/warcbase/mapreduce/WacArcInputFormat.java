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
import org.archive.io.arc.ARCReader;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.arc.ARCReaderFactory.CompressedARCReader;
import org.archive.io.arc.ARCRecord;
import org.warcbase.io.ArcRecordWritable;

public class WacArcInputFormat extends FileInputFormat<LongWritable, ArcRecordWritable> {
  @Override
  public RecordReader<LongWritable, ArcRecordWritable> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new ArcRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  public class ArcRecordReader extends RecordReader<LongWritable, ArcRecordWritable> {
    private ARCReader reader;
    private long start;
    private long pos;
    private long end;
    private LongWritable key = null;
    private ArcRecordWritable value = null;
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

      reader = (ARCReader) ARCReaderFactory.get(split.getPath().toString(),
          new BufferedInputStream(fileIn), true);

      iter = reader.iterator();
      //reader = (ARCReader) ARCReaderFactory.get(split.getPath().toString(), fileIn, true);

      this.pos = start;
    }

    private boolean isCompressedInput() {
      return reader instanceof CompressedARCReader;
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

      ARCRecord record = (ARCRecord) iter.next();
      if (record == null) {
        return false;
      }

      if (value == null) {
        value = new ArcRecordWritable();
      }
      value.setRecord(record);

      return true;
    }

    @Override
    public LongWritable getCurrentKey() {
      return key;
    }

    @Override
    public ArcRecordWritable getCurrentValue() {
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
