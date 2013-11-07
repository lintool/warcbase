package org.warcbase.browser.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.warcbase.data.Util;

public class WarcbaseServlet extends HttpServlet {
  private static final long serialVersionUID = 847405540723915805L;

  private final Configuration hbaseConfig;
  private String tableName;

  public WarcbaseServlet() {
    this.hbaseConfig = HBaseConfiguration.create();
  }


  
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String query = req.getParameter("query");
    String d = req.getParameter("date");
    WarcbaseResponse warcbaseResponse = new WarcbaseResponse();
    
    if(req.getPathInfo() == null || req.getPathInfo() == "/"){
      warcbaseResponse.writeTables(resp);
      return;
    }
    String pathInfo = req.getPathInfo();
    String[] splits = pathInfo.split("\\/");
    if(splits.length < 2){
      warcbaseResponse.writeTables(resp);
      return;
    }
    this.tableName = splits[1];

    if(splits.length == 2 && query == null){
      warcbaseResponse.tableSearch(resp, tableName);
      return;
    }
    if(splits.length == 2 && d == null){
      warcbaseResponse.writeDates(resp, tableName, query);
      return;
    }
    if(d == null)
      d = splits[2];
    if(query == null)
      query = pathInfo.substring(3 + splits[1].length() + splits[2].length(), pathInfo.length());

    String q = Util.reverseHostname(query);
    HTable table = new HTable(hbaseConfig, tableName);
    //System.out.println(q);
    Get get = new Get(Bytes.toBytes(q));
    Result rs = table.get(get);

    if (rs.raw().length == 0) {
      PrintWriter out = resp.getWriter();
      out.println("Not Found.");
      table.close();
      return;
    }
    if (d != null && d != "") {
      for (int i = 0; i < rs.raw().length; i++) {
        if(!(new String(rs.raw()[i].getFamily(), "UTF8").equals("content")))
          continue;
        String date = new String(rs.raw()[i].getQualifier());
        if (date.equals(d)) {
          warcbaseResponse.writeContent(resp, tableName, query, date, d);
          table.close();
          return;
        }
      }
      ArrayList<String> dates = new ArrayList<String>(10);
      for (int i = 0; i < rs.raw().length; i++)
        if(new String(rs.raw()[i].getFamily(), "UTF8").equals("content"))
          dates.add(new String(rs.raw()[i].getQualifier()));
      Collections.sort(dates);
      for (int i = 1; i < dates.size(); i++)
        if (dates.get(i).compareTo(d) > 0) {// d < i
          warcbaseResponse.writeContent(resp, tableName, query, dates.get(i), d);
          table.close();
          return;
        }
      int i = dates.size();
      warcbaseResponse.writeContent(resp, tableName, query, dates.get(i - 1), d);
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
      out.println("<br/> <a href='http://" + req.getServerName() + ":" + req.getServerPort() + req.getRequestURI() + "/" + date + "/" + query + "'>" + date + "</a>");
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
  
  public static void main(String[] args) {
  }
}
