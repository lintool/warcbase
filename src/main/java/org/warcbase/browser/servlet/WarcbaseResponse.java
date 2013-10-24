package org.warcbase.browser.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;

import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.warcbase.data.TextDocument2;
import org.warcbase.data.Util;

public class WarcbaseResponse {
  private final Configuration hbaseConfig;
  private HBaseAdmin hbaseAdmin;
  
  public WarcbaseResponse(){
    this.hbaseConfig = HBaseConfiguration.create();
    try {
      hbaseAdmin = new HBaseAdmin(hbaseConfig);
    } catch (MasterNotRunningException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
  
  public void writeTables(HttpServletResponse resp){
    HTableDescriptor[] htableDescriptors = null;
    try {
      htableDescriptors = hbaseAdmin.listTables();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    resp.setContentType("text/html");
    resp.setStatus(HttpServletResponse.SC_OK);
    PrintWriter out = null;
    try {
      out = resp.getWriter();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    out.println("<html>");
    out.println("<body>");
    for(HTableDescriptor htableDescriptor: htableDescriptors){
      String tableNameTmp = htableDescriptor.getNameAsString();
      out.println("<br/> <a href='" + TextDocument2.SERVER_PREFIX + tableNameTmp + "'>" + tableNameTmp + "</a>");
    }
    out.println("</body>");
    out.println("</html>");
  }
  
  public void tableSearch(HttpServletResponse resp, String tableName){
    resp.setContentType("text/html");
    resp.setStatus(HttpServletResponse.SC_OK);
    PrintWriter out = null;
    try {
      out = resp.getWriter();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
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

  public void writeDates(HttpServletResponse resp, String tableName, String query){
    String q = Util.reverseHostname(query);
    //System.out.println("query = " + q);
    HTable table = null;
    try {
      table = new HTable(hbaseConfig, tableName);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    Get get = new Get(Bytes.toBytes(q));
    Result rs = null;
    try {
      rs = table.get(get);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    byte[] data = null;
    resp.setContentType("text/html");
    resp.setStatus(HttpServletResponse.SC_OK);
    PrintWriter out = null;
    try {
      out = resp.getWriter();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    //System.out.println("num of results = " + rs.raw().length);
    out.println("<html>");
    out.println("<body>");
    if (rs.raw().length == 0) {
      out.println("Not Found.");
      out.println("<br/><a href='" + TextDocument2.SERVER_PREFIX + tableName + "'>" + "back to tableName" + "</a>");
    }
    else{
      for (int i = 0; i < rs.raw().length; i+=2) {
        String date = new String(rs.raw()[i].getQualifier());
        out.println("<br/> <a href='" + TextDocument2.SERVER_PREFIX + tableName + "/" + date + "/" + query + "'>" + date + "</a>");
      }
    }
    out.println("</body>");
    out.println("</html>");
    try {
      table.close();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
