package org.warcbase.browser.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Date;
import java.text.ParseException;
import java.util.Arrays;

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
import org.warcbase.data.HbaseManager;
import org.warcbase.data.TextDocument2;
import org.warcbase.data.UrlUtil;

public class WarcbaseServlet extends HttpServlet {
  private static final long serialVersionUID = 847405540723915805L;
  private static final Logger LOG = Logger.getLogger(WarcbaseServlet.class);

  private String tableName;

  private final Configuration hbaseConfig;
  private HBaseAdmin hbaseAdmin;
  private static HTablePool pool = new HTablePool();

  public WarcbaseServlet() throws MasterNotRunningException, ZooKeeperConnectionException {
    this.hbaseConfig = HBaseConfiguration.create();
    hbaseAdmin = new HBaseAdmin(hbaseConfig);
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String query = req.getParameter("query");
    String d = req.getParameter("date");

    LOG.info("Servlet called with: " + req);

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

    boolean nobanner = false;
    if (splits[2].equals("nobanner")) {
      nobanner = true;
    }

    if (d == null) {
      if (nobanner) {
        d = splits[2 + 1];
      } else {
        d = splits[2];
      }
    }
    
    if (query == null) {
      if (!nobanner) {
        query = pathInfo.substring(3 + splits[1].length() + splits[2].length(), pathInfo.length());
      } else {
        query = pathInfo.substring(4 + splits[1].length() + splits[2].length() + splits[3].length(), pathInfo.length());
      }
    }
    query = query.replace(" ", "%20");
    
    writeContent(resp, tableName, query, d);
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

      LOG.info("Retreiving " + key + " at " + date14digit);
      resp.setHeader("Content-Type", type);
      resp.setContentLength(data.length);
      resp.getOutputStream().write(data);
    }
    table.close();
  }
}
