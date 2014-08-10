package org.warcbase.browser.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Date;
import java.util.Arrays;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.warcbase.data.HbaseManager;
import org.warcbase.data.TextDocument2;
import org.warcbase.data.UrlUtil;

public class WarcbaseResponse {
  private final Configuration hbaseConfig;
  private HBaseAdmin hbaseAdmin;
  private static HTablePool pool = new HTablePool();

  public WarcbaseResponse() throws MasterNotRunningException, ZooKeeperConnectionException {
    this.hbaseConfig = HBaseConfiguration.create();
    hbaseAdmin = new HBaseAdmin(hbaseConfig);
  }

  public void writeTables(HttpServletResponse resp) throws IOException {
    HTableDescriptor[] htableDescriptors = null;
    htableDescriptors = hbaseAdmin.listTables();

    resp.setContentType("text/html");
    resp.setStatus(HttpServletResponse.SC_OK);
    PrintWriter out = null;
    out = resp.getWriter();

    out.println("<html>");
    out.println("<body>");
    for (HTableDescriptor htableDescriptor : htableDescriptors) {
      String tableNameTmp = htableDescriptor.getNameAsString();
      out.println("<br/> <a href='" + TextDocument2.SERVER_PREFIX + tableNameTmp + "'>"
          + tableNameTmp + "</a>");
    }
    out.println("</body>");
    out.println("</html>");
  }

  public void tableSearch(HttpServletResponse resp, String tableName) throws IOException {
    resp.setContentType("text/html");
    resp.setStatus(HttpServletResponse.SC_OK);
    PrintWriter out = null;
    out = resp.getWriter();

    out.println("<html>");
    out.println("<body>");
    out.println("<p>Enter URL:</p>");
    out.println("<form method=\"GET\" action=\"\"/>");
    out.println("<input name=\"query\" type=\"text\" size=\"100\"/><br/><br/>");
    out.println("<input type=\"submit\" value=\"Submit\" />");
    out.println("</form>");
    out.println("</body>");
    out.println("</html>");
  }

  public void writeDates(HttpServletResponse resp, String tableName, String query)
      throws IOException {
    String q = UrlUtil.urlToKey(query);
    HTableInterface table = pool.getTable(tableName);

    Get get = new Get(Bytes.toBytes(q));
    get.setMaxVersions(HbaseManager.MAX_VERSIONS);
    Result rs = null;
    rs = table.get(get);
    
    long[] dates = new long[rs.size()];
    for (int i = 0; i < rs.raw().length; i++)
      dates[i] = rs.raw()[i].getTimestamp();
    Arrays.sort(dates, 0, rs.raw().length);
    
    resp.setContentType("text/html");
    resp.setStatus(HttpServletResponse.SC_OK);
    PrintWriter out = null;
    out = resp.getWriter();

    out.println("<html>");
    out.println("<body>");
    if (rs.raw().length == 0) {
      out.println("Not Found.");
      out.println("<br/><a href='" + TextDocument2.SERVER_PREFIX + tableName + "'>" + "back to "
          + tableName + "</a>");
    } else {
      for (int i = 0; i < rs.raw().length; i++){
        //if (new String(rs.raw()[i].getFamily(), "UTF8").equals("content")) {
        String date = new Date(dates[i]).toString();
         out.println("<br/> <a href='" + TextDocument2.SERVER_PREFIX + tableName + "/" + dates[i]
              + "/" + query + "'>" + date + "</a>");
        //}
      }
    }
    out.println("</body>");
    out.println("</html>");
    table.close();
  }

  public void writeContent(HttpServletResponse resp, String tableName, String query, long d,
      long realDate, boolean nobanner) throws IOException {
    byte[] data = null;
    String type = null;
    String q = UrlUtil.urlToKey(query);
    HTableInterface table = pool.getTable(tableName);
    Get get = new Get(Bytes.toBytes(q));
    get.setMaxVersions(HbaseManager.MAX_VERSIONS);
    Result rs = null;
    rs = table.get(get);

    for (int i = 0; i < rs.raw().length; i++) {
      long timestamp = rs.raw()[i].getTimestamp();
      if (timestamp == d) {
        data = rs.raw()[i].getValue();
        type = Bytes.toString(rs.raw()[i].getQualifier());

        resp.setHeader("Content-Type", type);
        resp.setContentLength(data.length);
        resp.getOutputStream().write(data);

        break;
      }
    }

    table.close();
  }
}
