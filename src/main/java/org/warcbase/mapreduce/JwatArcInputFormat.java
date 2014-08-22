package org.warcbase.mapreduce;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.jwat.arc.ArcReader;
import org.jwat.arc.ArcReaderFactory;
import org.jwat.arc.ArcRecordBase;

public class JwatArcInputFormat extends FileInputFormat<LongWritable, ArcRecordBase> {
  @Override
  public RecordReader<LongWritable, ArcRecordBase> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new ArcRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  public class ArcRecordReader extends RecordReader<LongWritable, ArcRecordBase> {
    private CompressionCodecFactory compressionCodecs = null;
    private ArcReader reader;
    private long start;
    private long pos;
    private long end;
    private LongWritable key = null;
    private ArcRecordBase value = null;
    private Seekable filePosition;
    private CompressionCodec codec;
    private Decompressor decompressor;
    private DataInputStream in;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
      FileSplit split = (FileSplit) genericSplit;
      Configuration job = context.getConfiguration();
      start = split.getStart();
      end = start + split.getLength();
      final Path file = split.getPath();
      compressionCodecs = new CompressionCodecFactory(job);
      codec = compressionCodecs.getCodec(file);

      // open the file and seek to the start of the split
      FileSystem fs = file.getFileSystem(job);
      FSDataInputStream fileIn = fs.open(split.getPath());

      if (isCompressedInput()) {
        in = new DataInputStream(codec.createInputStream(fileIn, decompressor));
        filePosition = fileIn;
      } else {
        fileIn.seek(start);
        in = fileIn;
        filePosition = fileIn;
      }

      reader = ArcReaderFactory.getReader(in);
      this.pos = start;
    }

    private boolean isCompressedInput() {
      return (codec != null);
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
      if (key == null) {
        key = new LongWritable();
      }
      key.set(pos);

      value = reader.getNextRecord();
      if (value == null) {
        return false;
      }
      return true;
    }

    @Override
    public LongWritable getCurrentKey() {
      return key;
    }

    @Override
    public ArcRecordBase getCurrentValue() {
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
      try {
        if (in != null) {
          in.close();
        }
      } finally {
        if (decompressor != null) {
          CodecPool.returnDecompressor(decompressor);
        }
      }
    }
  }
}
