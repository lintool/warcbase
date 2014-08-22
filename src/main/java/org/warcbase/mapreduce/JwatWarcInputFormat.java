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
import org.jwat.common.UriProfile;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;

public class JwatWarcInputFormat extends FileInputFormat<LongWritable, WarcRecord> {
  private static final UriProfile uriProfile = UriProfile.RFC3986_ABS_16BIT_LAX;
  private static final boolean bBlockDigestEnabled = true;
  private static final boolean bPayloadDigestEnabled = true;
  private static final int recordHeaderMaxSize = 8192;
  private static final int payloadHeaderMaxSize = 32768;

  @Override
  public RecordReader<LongWritable, WarcRecord> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new WarcRecordReader();
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  public class WarcRecordReader extends RecordReader<LongWritable, WarcRecord> {
    private CompressionCodecFactory compressionCodecs = null;
    private WarcReader reader;
    private long start;
    private long pos;
    private long end;
    private LongWritable key = null;
    private WarcRecord value = null;
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

      reader = WarcReaderFactory.getReaderUncompressed(in);

      reader.setWarcTargetUriProfile(uriProfile);
      reader.setBlockDigestEnabled(bBlockDigestEnabled);
      reader.setPayloadDigestEnabled(bPayloadDigestEnabled);
      reader.setRecordHeaderMaxSize(recordHeaderMaxSize);
      reader.setPayloadHeaderMaxSize(payloadHeaderMaxSize);

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
    public WarcRecord getCurrentValue() {
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
