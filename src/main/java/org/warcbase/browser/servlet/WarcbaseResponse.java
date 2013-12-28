package org.warcbase.browser.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.warcbase.data.HbaseManager;
import org.warcbase.data.TextDocument2;
import org.warcbase.data.Util;

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
    String q = Util.reverseHostname(query);
    HTableInterface table = pool.getTable(tableName);

    Get get = new Get(Bytes.toBytes(q));
    Result rs = null;
    rs = table.get(get);

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
        String date = new Date(rs.raw()[i].getTimestamp()).toString();
         out.println("<br/> <a href='" + TextDocument2.SERVER_PREFIX + tableName + "/" + rs.raw()[i].getTimestamp()
              + "/" + query + "'>" + date + "</a>");
        //}
      }
    }
    out.println("</body>");
    out.println("</html>");
    table.close();
  }

  private void writeResponse(HttpServletResponse resp, Result rs, byte[] content, String query,
      long d, String type, int num, String tableName) throws IOException {
    if (type.startsWith("text/plain") || !type.startsWith("text")) {
      resp.setHeader("Content-Type", type);
      resp.setContentLength(content.length);
      resp.getOutputStream().write(content);
    } else {
      long[] dates = new long[HbaseManager.MAX_VERSIONS];
      for (int i = 0; i < rs.raw().length; i += 2)
        dates[i] = rs.raw()[i].getTimestamp();
      //Collections.sort(dates);
      Arrays.sort(dates, 0, rs.raw().length);
      long prevDate = 0, nextDate = 0;
      for (int i = 1; i < rs.raw().length; i++) {
        if (dates[i]  == d) {
          prevDate = dates[i - 1];
          if (i + 1 < dates.length)
            nextDate = dates[i + 1];
          else
            nextDate = d;
          break;
        }
        if (dates[i] > d) {// d < i
          if (i > 2)
            prevDate = dates[i - 2];
          else
            prevDate = dates[i - 1];
          nextDate = dates[i];
          break;
        }
      }
      System.setProperty("file.encoding", "UTF8");
      resp.setHeader("Content-Type", type);
      resp.setCharacterEncoding("UTF-8");
      PrintWriter out = resp.getWriter();
      TextDocument2 t2 = new TextDocument2(null, null, null);
      String bodyContent = new String(content, "UTF8");
      Document doc = Jsoup.parse(bodyContent);
      Element head = doc.select("head").first();
      Element base = doc.select("base").first();
      if (base == null) {
        head.prepend("<base id='warcbase-base-added' href='" + query + "'>");
      }
      bodyContent = doc.html();
      bodyContent = t2.fixURLs(bodyContent, query, String.valueOf(d), tableName);
      doc = Jsoup.parse(bodyContent);
      base = doc.select("base").first();
      if (base != null) {
        if (base.attr("id").equals("warcbase-base-added"))
          base.remove();
      }
      head = doc.select("head").first();
      head.prepend("<script type=\"text/javascript\"> function initYTVideo(id) {  _wmVideos_.init('/web/', id); } </script>  <script> function $(a){return document.getElementById(a)};     function addLoadEvent(a){if(window.addEventListener)addEventListener('load',a,false);else if(window.attachEvent)attachEvent('onload',a)} </script>");
      Element body = doc.select("body").first();
      body.prepend("<div id=\"wm-ipp\" style=\"display: block; position: relative; padding: 0px 5px; min-height: 70px; min-width: 800px; z-index: 9000;\"><div id=\"wm-ipp-inside\" style=\"position:fixed;padding:0!important;margin:0!important;width:97%;min-width:780px;border:5px solid #000;border-top:none;background-image:url("
          + TextDocument2.SERVER_PREFIX
          + "warcbase/"
          + "images/wm_tb_bk_trns.png);text-align:center;-moz-box-shadow:1px 1px 3px #333;-webkit-box-shadow:1px 1px 3px #333;box-shadow:1px 1px 3px #333;font-size:11px!important;font-family:'Lucida Grande','Arial',sans-serif!important;\">    <table style=\"border-collapse:collapse;margin:0;padding:0;width:100%;\"><tbody><tr>    <td style=\"padding:10px;vertical-align:top;min-width:110px;\">    <a href=\""
          + TextDocument2.SERVER_PREFIX
          + "\" title=\"Warcbase home page\" style=\"background-color:transparent;border:none;\">Warcbase</a>    </td>            <td style=\"padding:0!important;text-align:center;vertical-align:top;width:100%;\">         <table style=\"border-collapse:collapse;margin:0 auto;padding:0;width:570px;\"><tbody><tr>        <td style=\"padding:3px 0;\" colspan=\"2\">        <form target=\"_top\" method=\"get\" action=\""
          + TextDocument2.SERVER_PREFIX
          + tableName
          + "\" name=\"wmtb\" id=\"wmtb\" style=\"margin:0!important;padding:0!important;\"><input name=\"query\" id=\"wmtbURL\" value=\""
          + query
          + "\" style=\"width:400px;font-size:11px;font-family:'Lucida Grande','Arial',sans-serif;\" onfocus=\"javascript:this.focus();this.select();\" type=\"text\"><input name=\"date\" value=\"\" type=\"hidden\"><input name=\"type\" value=\"replay\" type=\"hidden\"><input name=\"date\" value=\"20120201185436\" type=\"hidden\"><input value=\"Go\" style=\"font-size:11px;font-family:'Lucida Grande','Arial',sans-serif;margin-left:5px;\" type=\"submit\"><span id=\"wm_tb_options\" style=\"display:block;\"></span></form>        </td>        <td style=\"vertical-align:bottom;padding:5px 0 0 0!important;\" rowspan=\"2\">            <table style=\"border-collapse:collapse;width:110px;color:#99a;font-family:'Helvetica','Lucida Grande','Arial',sans-serif;\"><tbody>                  <!-- NEXT/PREV MONTH NAV AND MONTH INDICATOR -->            <tr style=\"width:110px;height:16px;font-size:10px!important;\">              <td style=\"padding-right:9px;font-size:11px!important;font-weight:bold;text-transform:uppercase;text-align:right;white-space:nowrap;overflow:visible;\" nowrap=\"nowrap\">                                     <strong>PREV</strong>                                     </td>         <td style=\"padding-left:9px;font-size:11px!important;font-weight:bold;text-transform:uppercase;white-space:nowrap;overflow:visible;\" nowrap=\"nowrap\">                <strong>NEXT</strong>                                    </td>            </tr>             <!-- NEXT/PREV CAPTURE NAV AND DAY OF MONTH INDICATOR -->            <tr>                <td style=\"padding-right:9px;white-space:nowrap;overflow:visible;text-align:right!important;vertical-align:middle!important;\" nowrap=\"nowrap\">                                    <a href=\""
          + TextDocument2.SERVER_PREFIX
          + tableName
          + "/"
          + prevDate
          + "/"
          + query
          + "\" title=\""
          + prevDate
          + "\" style=\"background-color:transparent;border:none;\"><img src=\""
          + TextDocument2.SERVER_PREFIX
          + "warcbase/"
          + "images/wm_tb_prv_on.png\" alt=\"Previous capture\" border=\"0\" height=\"16\" width=\"14\"></a>                                    </td>         <td style=\"padding-left:9px;white-space:nowrap;overflow:visible;text-align:left!important;vertical-align:middle!important;\" nowrap=\"nowrap\">                                    <a href=\""
          + TextDocument2.SERVER_PREFIX
          + tableName
          + "/"
          + nextDate
          + "/"
          + query
          + "\" title=\""
          + nextDate
          + "\" style=\"background-color:transparent;border:none;\"><img src=\""
          + TextDocument2.SERVER_PREFIX
          + "warcbase/"
          + "images/wm_tb_nxt_on.png\" alt=\"Next capture\" border=\"0\" height=\"16\" width=\"14\"></a>                              </td>            </tr>             </tbody></table>        </td>         </tr>        <tr>        <td style=\"vertical-align:middle;padding:0!important;\">            <strong>"
          + " "
          + num
          + " captures"
          + "</strong>            <div style=\"margin:0!important;padding:0!important;color:#666;font-size:9px;padding-top:2px!important;white-space:nowrap;\" title=\"Timespan for captures of this URL\">"
          + new Date(dates[0]).toString()
          + "  -  "
          + new Date(dates[rs.raw().length - 1]).toString()
          + "</div>        </td>                </tr></tbody></table>    </td>    <td style=\"text-align:right;padding:5px;width:65px;font-size:11px!important;\">        <a href=\"javascript:;\" onclick=\"document.getElementById('wm-ipp').style.display='none';\" style=\"display:block;padding-right:18px;background:url("
          + TextDocument2.SERVER_PREFIX
          + "warcbase/"
          + "images/wm_tb_close.png) no-repeat 100% 0;color:#33f;font-family:'Lucida Grande','Arial',sans-serif;margin-bottom:23px;background-color:transparent;border:none;\" title=\"Close the toolbar\">Close</a>            </td>    </tr></tbody></table>  </div> </div>      <style type=\"text/css\">body{margin-top:0!important;padding-top:0!important;min-width:800px!important;}#wm-ipp a:hover{text-decoration:underline!important;}</style>");

      bodyContent = doc.html();
      out.println(bodyContent);
    }
  }

  public void writeContent(HttpServletResponse resp, String tableName, String query, long d,
      long realDate) throws IOException {
    byte[] data = null;
    String type = null;
    String q = Util.reverseHostname(query);
    HTableInterface table = pool.getTable(tableName);
    Get get = new Get(Bytes.toBytes(q));
    Result rs = null;
    rs = table.get(get);

    for (int i = 0; i < rs.raw().length; i++) {
      long timestamp = rs.raw()[i].getTimestamp();
      String date = new Date(timestamp).toString();
      if (timestamp == d) {
        data = rs.raw()[i].getValue();
        type = Bytes.toString(rs.raw()[i].getQualifier());
        break;
      }
    }

    writeResponse(resp, rs, data, query, realDate, type, rs.raw().length, tableName);
    table.close();
  }

}
