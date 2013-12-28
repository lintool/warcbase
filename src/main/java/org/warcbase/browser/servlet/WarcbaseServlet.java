package org.warcbase.browser.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
import org.warcbase.data.Util;

public class WarcbaseServlet extends HttpServlet {
  private static final long serialVersionUID = 847405540723915805L;
  private static final Logger LOG = Logger.getLogger(WarcbaseServlet.class);

  private String tableName;
  private static HTablePool pool = new HTablePool();
  private static WarcbaseResponse warcbaseResponse;

  public WarcbaseServlet() throws MasterNotRunningException, ZooKeeperConnectionException {
    warcbaseResponse = new WarcbaseResponse();
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException,
      IOException {
    String query = req.getParameter("query");
    String d = req.getParameter("date");

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

    if (d == null) {
      d = splits[2];
    }
    if (query == null) {
      query = pathInfo.substring(3 + splits[1].length() + splits[2].length(), pathInfo.length());
    }

    String q = Util.reverseHostname(query);
    HTableInterface table = pool.getTable(tableName);
    Get get = new Get(Bytes.toBytes(q));
    Result rs = table.get(get);

    if (rs.raw().length == 0) {
      PrintWriter out = resp.getWriter();
      out.println("Not Found.");
      table.close();
      return;
    }

    long dLong = Long.parseLong(d);
    if (d != null && d != "") {
      for (int i = 0; i < rs.raw().length; i++) {
        long timestamp = rs.raw()[i].getTimestamp();
        String date = new Date(timestamp).toString();
        if (timestamp == dLong) {
          warcbaseResponse.writeContent(resp, tableName, query, timestamp, dLong);
          table.close();
          return;
        }
      }

      long[] dates = new long[HbaseManager.MAX_VERSIONS];
      for (int i = 0; i < rs.raw().length; i++)
        dates[i] = rs.raw()[i].getTimestamp();
      Arrays.sort(dates, 0, rs.raw().length);
      for (int i = 1; i < rs.raw().length; i++)
        if (dates[i] > dLong) {// d < i
          warcbaseResponse.writeContent(resp, tableName, query, dates[i], dLong);
          table.close();
          return;
        }
      int i = rs.raw().length;
      warcbaseResponse.writeContent(resp, tableName, query, dates[i - 1], dLong);
      table.close();
      return;
    }

    resp.setContentType("text/html");
    resp.setStatus(HttpServletResponse.SC_OK);
    PrintWriter out = resp.getWriter();
    out.println("<html>");
    out.println("<body>");
    for (int i = 0; i < rs.raw().length; i++) {
      String date = new String(rs.raw()[i].getQualifier());
      out.println("<br/> <a href='http://" + req.getServerName() + ":" + req.getServerPort()
          + req.getRequestURI() + "/" + date + "/" + query + "'>" + date + "</a>");
    }
    out.println("</body>");
    out.println("</html>");
    table.close();
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
