package org.warcbase.browser;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Date;
import java.text.ParseException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.archive.util.ArchiveUtils;
import org.warcbase.data.HBaseTableManager;
import org.warcbase.data.UrlUtils;

public class WarcBrowserServlet extends HttpServlet {
  private static final long serialVersionUID = 847405540723915805L;
  private static final Logger LOG = Logger.getLogger(WarcBrowserServlet.class);

  private final Configuration hbaseConfig;
  private HBaseAdmin hbaseAdmin;
  private HConnection hbaseConnection;

  private final Pattern p1 = Pattern.compile("^/([^//]+)/(\\d+)/(http://.*)$");
  private final Pattern p2 = Pattern.compile("^/([^//]+)/\\*/(http://.*)$");

  public WarcBrowserServlet() throws
      IOException, MasterNotRunningException, ZooKeeperConnectionException {
    this.hbaseConfig = HBaseConfiguration.create();
    hbaseAdmin = new HBaseAdmin(hbaseConfig);
    hbaseConnection = HConnectionManager.createConnection(hbaseConfig);
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String path = req.getPathInfo();
    if (req.getQueryString() != null) {
        path = path + "?" + req.getQueryString();
    }

    LOG.info("Servlet called: " + path);
    Matcher m1 = p1.matcher(path);
    if (m1.find()) {
      // collection, url, 14 digit date
      String url = m1.group(3);
      url = url.replaceAll(" ", "%20");
      writeContent(resp, m1.group(1), url, m1.group(2));
      return;
    }

    Matcher m2 = p2.matcher(path);
    if (m2.find()) {
      String url = m2.group(2);
      url = url.replaceAll(" ", "%20");
      writeCaptureDates(resp, m2.group(1), url);
      return;
    }

    // Otherwise, just list the dates of the available collections
    listCollections(resp);
  }

  protected void doPost(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    PrintWriter out = resp.getWriter();
    out.println("<html>");
    out.println("<body>");
    out.println("Sorry, only GET is supported.");
    out.println("</body>");
    out.println("</html>");
    out.close();
  }

  public void listCollections(HttpServletResponse resp) throws IOException {
    HTableDescriptor[] htableDescriptors = null;
    htableDescriptors = hbaseAdmin.listTables();

    resp.setContentType("text/plain");
    resp.setStatus(HttpServletResponse.SC_OK);
    PrintWriter out = resp.getWriter();

    for (HTableDescriptor htableDescriptor : htableDescriptors) {
      String tableNameTmp = htableDescriptor.getNameAsString();
      out.println(tableNameTmp);
    }
  }

  public void writeCaptureDates(HttpServletResponse resp, String tableName, String query)
      throws IOException {
    String q = UrlUtils.urlToKey(query);
    HTableInterface table = hbaseConnection.getTable(tableName);

    Get get = new Get(Bytes.toBytes(q));
    get.setMaxVersions(HBaseTableManager.MAX_VERSIONS);
    Result result = table.get(get);
   
    String type = null; 
    long[] dates = new long[result.size()];
    Cell[] cells = result.rawCells();
    for (int i = 0; i < cells.length; i++) {
      dates[i] = cells[i].getTimestamp();
      if (type == null && cells[i] != null) {
        type = Bytes.toString(CellUtil.cloneQualifier(cells[i]));
      }
    }
    Arrays.sort(dates, 0, cells.length);
    // Will the versions diff in type?

    resp.setContentType("text/plain");
    resp.setStatus(HttpServletResponse.SC_OK);
    PrintWriter out = resp.getWriter();

    for (int i = 0; i < cells.length; i++) {
      String date14digit = ArchiveUtils.get14DigitDate(new Date(dates[i]));
      out.println(date14digit + "\t" + type + "\t" + "/" + tableName + "/" + date14digit + "/" + query);
    }
    table.close();
  }

  public void writeContent(HttpServletResponse resp, String tableName, String url, String date14digit) 
      throws IOException {
    String key = UrlUtils.urlToKey(url);
    HTableInterface table = hbaseConnection.getTable(tableName);
    Get get = new Get(Bytes.toBytes(key));
    try {
      get.setTimeStamp(ArchiveUtils.parse14DigitDate(date14digit).getTime());
    } catch (ParseException e) {
      e.printStackTrace();
    }
    Result result = table.get(get);
    Cell[] cells = result.rawCells();

    if (cells.length == 1) {
      // We should have exactly one result here...
      byte[] data = CellUtil.cloneValue(cells[0]);
      String type = Bytes.toString(CellUtil.cloneQualifier(cells[0]));

      LOG.info("Fetching " + key + " at " + date14digit);
      resp.setHeader("Content-Type", type);
      resp.setContentLength(data.length);
      resp.getOutputStream().write(data);
    }
    table.close();
  }
}
