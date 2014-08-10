package org.warcbase.browser.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.warcbase.data.HbaseManager;
import org.warcbase.data.UrlUtil;

public class WarcbaseServlet extends HttpServlet {
  private static final long serialVersionUID = 847405540723915805L;
  private static final Logger LOG = Logger.getLogger(WarcbaseServlet.class);

  private String tableName;
  private static HTablePool pool = new HTablePool();
  private static WarcbaseResponse warcbaseResponse;

  public WarcbaseServlet() throws MasterNotRunningException, ZooKeeperConnectionException {
    warcbaseResponse = new WarcbaseResponse();
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String query = req.getParameter("query");
    String d = req.getParameter("date");

    LOG.info("Servlet called with: " + req);

    if (req.getPathInfo() == null || req.getPathInfo() == "/") {
      warcbaseResponse.writeTables(resp);
      return;
    }
    String pathInfo = req.getPathInfo();
    String[] splits = pathInfo.split("\\/");

    if (splits.length < 2) {
      warcbaseResponse.writeTables(resp);
      return;
    }
    this.tableName = splits[1];

    // Request has table name, but not URL.
    if (splits.length == 2 && query == null) {
      warcbaseResponse.tableSearch(resp, tableName);
      return;
    }

    // If there isn't a date for a URL, print out list of available versions.
    if (splits.length == 2 && d == null) {
      warcbaseResponse.writeDates(resp, tableName, query);
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
    
    String q = UrlUtil.urlToKey(query);
    HTableInterface table = pool.getTable(tableName);
    Get get = new Get(Bytes.toBytes(q));
    get.setMaxVersions(HbaseManager.MAX_VERSIONS);
    Result rs = table.get(get);

    if (rs.raw().length == 0) {
      PrintWriter out = resp.getWriter();
      out.println("Not Found.");
      table.close();
      return;
    }

    long dLong = Long.parseLong(d);
    warcbaseResponse.writeContent(resp, tableName, query, dLong);
  }

  protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
      IOException {
    String field = req.getParameter("field");
    PrintWriter out = resp.getWriter();

    out.println("<html>");
    out.println("<body>");
    out.println("You entered \"" + field + "\" into the text box.");
    out.println("</body>");
    out.println("</html>");
  }
  
}
