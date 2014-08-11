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
import org.apache.log4j.Logger;
import org.archive.util.ArchiveUtils;
import org.warcbase.data.HBaseTableManager;
import org.warcbase.data.UrlUtil;

public class WarcBrowserServlet extends HttpServlet {
  private static final long serialVersionUID = 847405540723915805L;
  private static final Logger LOG = Logger.getLogger(WarcBrowserServlet.class);

  private String tableName;

  private final Configuration hbaseConfig;
  private HBaseAdmin hbaseAdmin;
  private static HTablePool pool = new HTablePool();

  private final Pattern p1 = Pattern.compile("^/([^//]+)/(\\d+)/(http://.*)$");
  private final Pattern p2 = Pattern.compile("^/([^//]+)/\\*/(http://.*)$");

  public WarcBrowserServlet() throws MasterNotRunningException, ZooKeeperConnectionException {
    this.hbaseConfig = HBaseConfiguration.create();
    hbaseAdmin = new HBaseAdmin(hbaseConfig);
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String query = req.getParameter("query");
    String d = req.getParameter("date");

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
    }

    Matcher m2 = p2.matcher(path);
    if (m2.find()) {
      String url = m2.group(2);
      url = url.replaceAll(" ", "%20");
      writeDates(resp, m2.group(1), url);
    }

    if (req.getPathInfo() == null || req.getPathInfo() == "/") {
      writeTables(resp);
      return;
    }
    String pathInfo = req.getPathInfo();
    String[] splits = pathInfo.split("\\/");

    if (splits.length < 2) {
      writeTables(resp);
      return;
    }
    this.tableName = splits[1];

    // Request has table name, but not URL.
    if (splits.length == 2 && query == null) {
      tableSearch(resp, tableName);
      return;
    }

    // If there isn't a date for a URL, print out list of available versions.
    if (splits.length == 2 && d == null) {
      writeDates(resp, tableName, query);
      return;
    }
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
      out.println("<br/>" + tableNameTmp + tableNameTmp + "</a>");
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
    get.setMaxVersions(HBaseTableManager.MAX_VERSIONS);
    Result rs = null;
    rs = table.get(get);
   
    String type = null; 
    long[] dates = new long[rs.size()];
    for (int i = 0; i < rs.raw().length; i++) {
      dates[i] = rs.raw()[i].getTimestamp();
      if (type == null && rs.raw()[i] != null) {
	  type = Bytes.toString(rs.raw()[i].getQualifier());
      }
    }
    Arrays.sort(dates, 0, rs.raw().length);
    // Will the versions diff in type?

    resp.setContentType("text/plain");
    resp.setStatus(HttpServletResponse.SC_OK);
    PrintWriter out = null;
    out = resp.getWriter();

    for (int i = 0; i < rs.raw().length; i++) {
      String date14digit = ArchiveUtils.get14DigitDate(new Date(dates[i]));
      out.println(date14digit + "\t" + type + "\t" + "/" + tableName + "/" + date14digit + "/" + query);
    }
    table.close();
  }

  public void writeContent(HttpServletResponse resp, String tableName, String url, String date14digit) 
      throws IOException {
    String key = UrlUtil.urlToKey(url);
    HTableInterface table = pool.getTable(tableName);
    Get get = new Get(Bytes.toBytes(key));
    try {
      get.setTimeStamp(ArchiveUtils.parse14DigitDate(date14digit).getTime());
    } catch (ParseException e) {
      e.printStackTrace();
    }
    Result rs = null;
    rs = table.get(get);

    if (rs.raw().length == 1) {
      // We should have exactly one result here...
      byte[] data = rs.raw()[0].getValue();
      String type = Bytes.toString(rs.raw()[0].getQualifier());

      LOG.info("Fetching " + key + " at " + date14digit);
      resp.setHeader("Content-Type", type);
      resp.setContentLength(data.length);
      resp.getOutputStream().write(data);
    }
    table.close();
  }
}
