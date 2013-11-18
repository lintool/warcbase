package org.warcbase.data;

import java.lang.reflect.Field;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.warcbase.ingest.IngestFiles;

public class HbaseManager {
  private static final String[] FAMILIES = { "content", "type" };
  private static final Logger LOG = Logger.getLogger(HbaseManager.class);
  private static final int MAX_KEY_VALUE_SIZE = IngestFiles.MAX_CONTENT_SIZE + 200;

  private final HTable table;
  private final HBaseAdmin admin;

  public HbaseManager(String name, boolean create) throws Exception {
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

      HTableDescriptor tableDesc = new HTableDescriptor(name);
      for (int i = 0; i < FAMILIES.length; i++) {
        tableDesc.addFamily(new HColumnDescriptor(FAMILIES[i]));
      }
      admin.createTable(tableDesc);
      LOG.info(String.format("Successfully created table '%s'", name));
    }

    table = new HTable(hbaseConfig, name);
    // TODO: This doesn't seem right.
    // Look in HBase book to see how you can set table parameters programmatically.
    Field maxKeyValueSizeField = HTable.class.getDeclaredField("maxKeyValueSize");
    maxKeyValueSizeField.setAccessible(true);
    maxKeyValueSizeField.set(table, MAX_KEY_VALUE_SIZE);

    LOG.info("Setting maxKeyValueSize to " + maxKeyValueSizeField.get(table));
    admin.close();
  }

  public boolean addRecord(String key, String date, byte[] data, String type) {
    try {
      Put put = new Put(Bytes.toBytes(key));
      put.add(Bytes.toBytes(FAMILIES[0]), Bytes.toBytes(date), data);
      put.add(Bytes.toBytes(FAMILIES[1]), Bytes.toBytes(date), Bytes.toBytes(type));
      table.put(put);
      return true;
    } catch (Exception e) {
      LOG.error("Couldn't insert key: " + key);
      LOG.error("File Size: " + data.length);
      LOG.error(e.getMessage());
      e.printStackTrace();
      return false;
    }
  }
}
