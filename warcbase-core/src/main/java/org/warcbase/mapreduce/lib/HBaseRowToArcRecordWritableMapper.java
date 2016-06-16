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

package org.warcbase.mapreduce.lib;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.warcbase.data.ArcRecordUtils;
import org.warcbase.io.ArcRecordWritable;

public class HBaseRowToArcRecordWritableMapper
    extends Mapper<ImmutableBytesWritable, Result, LongWritable, ArcRecordWritable> {
  public static enum Rows { TOTAL };

  private final LongWritable keyOut = new LongWritable();
  private final ArcRecordWritable valueOut = new ArcRecordWritable();

  @Override
  public void map(ImmutableBytesWritable row, Result result, Context context)
      throws IOException, InterruptedException {
    context.getCounter(Rows.TOTAL).increment(1);

    for (Cell kv : result.listCells()) {
      valueOut.setRecord(ArcRecordUtils.fromBytes(CellUtil.cloneValue(kv)));
      context.write(keyOut, valueOut);
    }
  }
}