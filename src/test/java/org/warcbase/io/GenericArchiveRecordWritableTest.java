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

package org.warcbase.io;

import com.google.common.io.Resources;
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
import org.apache.ivy.osgi.updatesite.xml.Archive;
import org.archive.io.arc.ARCRecord;
import org.archive.io.arc.ARCRecordMetaData;
import org.junit.Test;
import org.warcbase.io.GenericArchiveRecordWritable.ArchiveFormat;
import org.warcbase.mapreduce.WacArcInputFormat;
import org.warcbase.mapreduce.WacGenericInputFormat;

import java.io.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GenericArchiveRecordWritableTest {
    @Test
    public void testArcInputFormat() throws Exception {
        String arcFile = Resources.getResource("arc/example.arc.gz").getPath();

        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");

        File testFile = new File(arcFile);
        Path path = new Path(testFile.getAbsoluteFile().toURI());
        FileSplit split = new FileSplit(path, 0, testFile.length(), null);

        InputFormat<LongWritable, GenericArchiveRecordWritable> inputFormat = ReflectionUtils.newInstance(
                WacGenericInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, GenericArchiveRecordWritable> reader = inputFormat.createRecordReader(split,
                context);

        reader.initialize(split, context);

        int cnt = 0;
        while (reader.nextKeyValue()) {
            GenericArchiveRecordWritable record = reader.getCurrentValue();
            cnt++;

            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(bytesOut);

            record.write(dataOut);

            GenericArchiveRecordWritable reconstructed = new GenericArchiveRecordWritable();

            reconstructed.setFormat(ArchiveFormat.ARC);
            reconstructed.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

            boolean isArc = (record.getFormat() == ArchiveFormat.ARC);
            assertEquals(isArc, true);
            if (isArc) {
                assertEquals(((ARCRecord) record.getRecord()).getMetaData().getUrl(),
                        ((ARCRecord) reconstructed.getRecord()).getMetaData().getUrl());
            }
        }

        assertEquals(300, cnt);
    }

    @Test
    public void testWarcInputFormat() throws Exception {
        String warcFile = Resources.getResource("warc/example.warc.gz").getPath();

        Configuration conf = new Configuration(false);
        conf.set("fs.defaultFS", "file:///");

        File testFile = new File(warcFile);
        Path path = new Path(testFile.getAbsoluteFile().toURI());
        FileSplit split = new FileSplit(path, 0, testFile.length(), null);

        InputFormat<LongWritable, GenericArchiveRecordWritable> inputFormat = ReflectionUtils.newInstance(
                WacGenericInputFormat.class, conf);
        TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
        RecordReader<LongWritable, GenericArchiveRecordWritable> reader = inputFormat.createRecordReader(split,
                context);

        reader.initialize(split, context);

        int cnt = 0;
        while (reader.nextKeyValue()) {
            GenericArchiveRecordWritable record = reader.getCurrentValue();

            cnt++;

            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            DataOutputStream dataOut = new DataOutputStream(bytesOut);

            record.write(dataOut);

            GenericArchiveRecordWritable reconstructed = new GenericArchiveRecordWritable();

            reconstructed.setFormat(ArchiveFormat.WARC);
            reconstructed.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

            boolean isWarc = (record.getFormat() == ArchiveFormat.WARC);
            assertTrue(isWarc);
            if (isWarc) {
                assertEquals(record.getRecord().getHeader().getUrl(),
                        reconstructed.getRecord().getHeader().getUrl());
                assertEquals(record.getRecord().getHeader().getContentLength(),
                        reconstructed.getRecord().getHeader().getContentLength());
            }
        }

        assertEquals(822, cnt);
    }
}
