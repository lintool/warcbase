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

package org.warcbase.data;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.archive.util.ArchiveUtils;
import org.warcbase.ingest.IngestFiles;

public class HBaseTableManager {
  private static final Logger LOG = Logger.getLogger(HBaseTableManager.class);

  private static final String[] FAMILIES = { "c" };
  private static final int MAX_KEY_VALUE_SIZE = IngestFiles.MAX_CONTENT_SIZE + 1024;
  // Add a bit of padding for headers, etc.
  public static final int MAX_VERSIONS = Integer.MAX_VALUE;

  private final HTable table;
  private final HBaseAdmin admin;

  public HBaseTableManager(String name, boolean create, Compression.Algorithm compression) throws Exception {
    Configuration hbaseConfig = HBaseConfiguration.create();
    admin = new HBaseAdmin(hbaseConfig);

    if (admin.tableExists(name) && !create) {
      LOG.info(String.format("Table '%s' exists: doing nothing.", name));
    } else {
      if (admin.tableExists(name)) {
        LOG.info(String.format("Table '%s' exists: dropping table and recreating.", name));
        LOG.info(String.format("Disabling table '%s'", name));
        admin.disableTable(name);
        LOG.info(String.format("Droppping table '%s'", name));
        admin.deleteTable(name);
      }

      HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(name));
      for (int i = 0; i < FAMILIES.length; i++) {
        HColumnDescriptor hColumnDesc = new HColumnDescriptor(FAMILIES[i]);
        hColumnDesc.setMaxVersions(MAX_VERSIONS);
        hColumnDesc.setCompressionType(compression);
        hColumnDesc.setCompactionCompressionType(compression);
        hColumnDesc.setTimeToLive(HConstants.FOREVER);
        tableDesc.addFamily(hColumnDesc);
      }
      admin.createTable(tableDesc);
      LOG.info(String.format("Successfully created table '%s'", name));
    }

    table = new HTable(hbaseConfig, name);
    Field maxKeyValueSizeField = HTable.class.getDeclaredField("maxKeyValueSize");
    maxKeyValueSizeField.setAccessible(true);
    maxKeyValueSizeField.set(table, MAX_KEY_VALUE_SIZE);

    LOG.info("Setting maxKeyValueSize to " + maxKeyValueSizeField.get(table));
    admin.close();
  }

  public boolean insertRecord(final String key, final String date14digits,
      final byte[] data, final String type) {
    try {
      long timestamp = ArchiveUtils.parse14DigitDate(date14digits).getTime();
      Put put = new Put(Bytes.toBytes(key));
      put.setDurability(Durability.SKIP_WAL);
      put.add(Bytes.toBytes(FAMILIES[0]), Bytes.toBytes(type), timestamp, data);
      table.put(put);
      return true;
    } catch (Exception e) {
      LOG.error("Couldn't insert key: " + key + ", size: " + data.length);
      LOG.error(e.getMessage());
      e.printStackTrace();
      return false;
    }
  }
}
