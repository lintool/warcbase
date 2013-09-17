package org.warcbase.browser.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.warcbase.data.HttpResponseRecord;
import org.warcbase.data.TextDocument2;
import org.warcbase.data.Util;

public class WarcbaseServlet extends HttpServlet {
  private static final long serialVersionUID = 847405540723915805L;

  private final Configuration hbaseConfig;
  private final String name;

  public WarcbaseServlet(String name) {
    this.hbaseConfig = HBaseConfiguration.create();
    this.name = name;
  }

  private void writeResponse(HttpServletResponse resp, Result rs, byte[] data, String query, String d, int num)
      throws IOException {
    HttpResponseRecord httpResponseRecord = new HttpResponseRecord(data);
    
    //String content = new String(data, "UTF8");
    
    //System.out.println("\n" + content + "\n");
    //System.out.println(warcRecordParser.getType());
    

    if (!httpResponseRecord.getType().startsWith("text")) {
      resp.setHeader("Content-Type", httpResponseRecord.getType());
      resp.setContentLength(httpResponseRecord.getBodyByte().length);
      resp.getOutputStream().write(httpResponseRecord.getBodyByte());
    } else {
      ArrayList<String> dates = new ArrayList<String>(10);
      for (int i = 0; i < rs.raw().length; i++)
        dates.add(new String(rs.raw()[i].getQualifier()));
      Collections.sort(dates);
      String prevDate = null, nextDate = null;
      for (int i = 1; i < dates.size(); i++){
        if (dates.get(i).compareTo(d) == 0){
          prevDate = dates.get(i - 1);
          if (i + 1 < dates.size())
            nextDate = dates.get(i + 1);
          else
            nextDate = d;
          break;
        }
        if (dates.get(i).compareTo(d) > 0) {// d < i
          if ( i > 2)
            prevDate = dates.get(i - 2);
          else
            prevDate = dates.get(i - 1);
          nextDate = dates.get(i);
          break;
        }
      }
      System.setProperty("file.encoding", "UTF8");
      resp.setHeader("Content-Type", httpResponseRecord.getType());
      resp.setCharacterEncoding("UTF-8");
      PrintWriter out = resp.getWriter();
      TextDocument2 t2 = new TextDocument2(null, null, null);
      String bodyContent = new String(httpResponseRecord.getBodyByte(), "UTF8");
      //System.out.println(query);
      //System.out.println(query.replaceAll("&amp;", "&"));      
      bodyContent = t2.fixURLs(bodyContent, query, d);
      Document doc = Jsoup.parse(bodyContent);
      //Elements bodies = doc.getElementsByTag("body");
      //Elements heads = doc.getElementsByTag("head");
      Element head = doc.select("head").first();
      head.prepend("<script type='text/javascript'> function initYTVideo(id) {  _wmVideos_.init('/web/', id); } </script>  <script> function $(a){return document.getElementById(a)};     function addLoadEvent(a){if(window.addEventListener)addEventListener('load',a,false);else if(window.attachEvent)attachEvent('onload',a)} </script>");
      /*for(Element body : bodies){
        String bodyText = body.html();
        System.out.println(bodyText);
      }*/
      Element body = doc.select("body").first();
      body.prepend("<div id=\"wm-ipp\" style=\"display: block; position: relative; padding: 0px 5px; min-height: 70px; min-width: 800px; z-index: 9000;\"><div id=\"wm-ipp-inside\" style=\"position:fixed;padding:0!important;margin:0!important;width:97%;min-width:780px;border:5px solid #000;border-top:none;background-image:url(images/wm_tb_bk_trns.png);text-align:center;-moz-box-shadow:1px 1px 3px #333;-webkit-box-shadow:1px 1px 3px #333;box-shadow:1px 1px 3px #333;font-size:11px!important;font-family:'Lucida Grande','Arial',sans-serif!important;\">    <table style=\"border-collapse:collapse;margin:0;padding:0;width:100%;\"><tbody><tr>    <td style=\"padding:10px;vertical-align:top;min-width:110px;\">    <a href=\""
          //+ "http://web.archive.org/web/"
          + TextDocument2.SERVER_PREFIX + "warcbase"
          + "\" title=\"Warcbase home page\" style=\"background-color:transparent;border:none;\">Warcbase</a>    </td>            <td style=\"padding:0!important;text-align:center;vertical-align:top;width:100%;\">         <table style=\"border-collapse:collapse;margin:0 auto;padding:0;width:570px;\"><tbody><tr>        <td style=\"padding:3px 0;\" colspan=\"2\">        <form target=\"_top\" method=\"get\" action=\"servlet\" name=\"wmtb\" id=\"wmtb\" style=\"margin:0!important;padding:0!important;\"><input name=\"query\" id=\"wmtbURL\" value=\""
              //+ "http://www.wikipedia.org/"
              + query
              + "\" style=\"width:400px;font-size:11px;font-family:'Lucida Grande','Arial',sans-serif;\" onfocus=\"javascript:this.focus();this.select();\" type=\"text\"><input name=\"date\" value=\"\" type=\"hidden\"><input name=\"type\" value=\"replay\" type=\"hidden\"><input name=\"date\" value=\"20120201185436\" type=\"hidden\"><input value=\"Go\" style=\"font-size:11px;font-family:'Lucida Grande','Arial',sans-serif;margin-left:5px;\" type=\"submit\"><span id=\"wm_tb_options\" style=\"display:block;\"></span></form>        </td>        <td style=\"vertical-align:bottom;padding:5px 0 0 0!important;\" rowspan=\"2\">            <table style=\"border-collapse:collapse;width:110px;color:#99a;font-family:'Helvetica','Lucida Grande','Arial',sans-serif;\"><tbody>                  <!-- NEXT/PREV MONTH NAV AND MONTH INDICATOR -->            <tr style=\"width:110px;height:16px;font-size:10px!important;\">              <td style=\"padding-right:9px;font-size:11px!important;font-weight:bold;text-transform:uppercase;text-align:right;white-space:nowrap;overflow:visible;\" nowrap=\"nowrap\">                                     <strong>PREV</strong>                                     </td>         <td style=\"padding-left:9px;font-size:11px!important;font-weight:bold;text-transform:uppercase;white-space:nowrap;overflow:visible;\" nowrap=\"nowrap\">                <strong>NEXT</strong>                                    </td>            </tr>             <!-- NEXT/PREV CAPTURE NAV AND DAY OF MONTH INDICATOR -->            <tr>                <td style=\"padding-right:9px;white-space:nowrap;overflow:visible;text-align:right!important;vertical-align:middle!important;\" nowrap=\"nowrap\">                                    <a href=\""
                  //+ "http://web.archive.org/web/20120201165858/http://www.wikipedia.org/"
                  + TextDocument2.SERVER_PREFIX + "warcbase/servlet?date=" + prevDate + "&query=" + query
                  + "\" title=\""
                  //+ "16:58:58 Feb 1, 2012"
                  + prevDate
                  + "\" style=\"background-color:transparent;border:none;\"><img src=\"images/wm_tb_prv_on.png\" alt=\"Previous capture\" border=\"0\" height=\"16\" width=\"14\"></a>                                    </td>         <td style=\"padding-left:9px;white-space:nowrap;overflow:visible;text-align:left!important;vertical-align:middle!important;\" nowrap=\"nowrap\">                                    <a href=\""
                      //+ "http://web.archive.org/web/20120201230139/http://www.wikipedia.org/"
                      + TextDocument2.SERVER_PREFIX + "warcbase/servlet?date=" + nextDate + "&query=" + query
                      + "\" title=\""
                      //+ "23:01:39 Feb 1, 2012"
                      + nextDate
                      + "\" style=\"background-color:transparent;border:none;\"><img src=\"images/wm_tb_nxt_on.png\" alt=\"Next capture\" border=\"0\" height=\"16\" width=\"14\"></a>                              </td>            </tr>             </tbody></table>        </td>         </tr>        <tr>        <td style=\"vertical-align:middle;padding:0!important;\">            <strong>"
                          + " " + num + " captures"
                          + "</strong>            <div style=\"margin:0!important;padding:0!important;color:#666;font-size:9px;padding-top:2px!important;white-space:nowrap;\" title=\"Timespan for captures of this URL\">"
                              //+ "27 Jul 01 - 23 Jun 13"
                              + dates.get(0).substring(0, 10) + "  -  " + dates.get(dates.size() - 1).substring(0, 10)
                              + "</div>        </td>                </tr></tbody></table>    </td>    <td style=\"text-align:right;padding:5px;width:65px;font-size:11px!important;\">        <a href=\"javascript:;\" onclick=\"document.getElementById('wm-ipp').style.display='none';\" style=\"display:block;padding-right:18px;background:url(images/wm_tb_close.png) no-repeat 100% 0;color:#33f;font-family:'Lucida Grande','Arial',sans-serif;margin-bottom:23px;background-color:transparent;border:none;\" title=\"Close the toolbar\">Close</a>            </td>    </tr></tbody></table>  </div> </div>      <style type=\"text/css\">body{margin-top:0!important;padding-top:0!important;min-width:800px!important;}#wm-ipp a:hover{text-decoration:underline!important;}</style>");

      
      
      //System.out.println(head.html());
      bodyContent = doc.html();
      out.println(bodyContent);
    }
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String query = req.getParameter("query");
    //System.out.println("\n" + query + "\n");
    
    String d = req.getParameter("date");

    String q = Util.reverseHostname(query);
    HTable table = new HTable(hbaseConfig, name);
    //System.out.println(q);
    Get get = new Get(Bytes.toBytes(q));
    Result rs = table.get(get);
    byte[] data = null;

    /*for (int i = 1; i < rs.raw().length; i++) {
      System.out.println(rs.raw()[i].getValue().length + " " + rs.raw()[i - 1].getValue().length);
      if (Arrays.equals(ResponseRecord.getBodyByte(rs.raw()[i].getValue()),
          ResponseRecord.getBodyByte(rs.raw()[i - 1].getValue())))
        System.out.println("++++++++++++=================++++++++++++++");
    }*/

    if (rs.raw().length == 0) {
      PrintWriter out = resp.getWriter();
      out.println("Not Found.");
      table.close();
      return;
    }
    if (d != null && d != "") {
      for (int i = 0; i < rs.raw().length; i++) {
        String date = new String(rs.raw()[i].getQualifier());
        if (date.equals(d)) {
          data = rs.raw()[i].getValue();
          writeResponse(resp, rs, data, query, d, rs.raw().length);
          table.close();
          return;
        }
      }
      ArrayList<String> dates = new ArrayList<String>(10);
      for (int i = 0; i < rs.raw().length; i++)
        dates.add(new String(rs.raw()[i].getQualifier()));
      Collections.sort(dates);
      for (int i = 1; i < dates.size(); i++)
        if (dates.get(i).compareTo(d) > 0) {// d < i
          data = rs.raw()[i - 1].getValue();
          writeResponse(resp, rs, data, query, d, rs.raw().length);
          table.close();
          return;
        }
      int i = dates.size();
      data = rs.raw()[i - 1].getValue();
      writeResponse(resp, rs, data, query, d, rs.raw().length);
      table.close();
      return;
    }

    PrintWriter out = resp.getWriter();
    out.println("<html>");
    out.println("<body>");
    for (int i = 0; i < rs.raw().length; i++) {
      String date = new String(rs.raw()[i].getQualifier());
      out.println("<br/> <a href='http://" + req.getServerName() + ":" + req.getServerPort()
          + req.getRequestURI() + "?query=" + URLEncoder.encode(req.getParameter("query"), "US-ASCII") + "&date=" + date + "'>"
          + date + "</a>");
      //URLEncoder.encode(req.getParameter("query")
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
    String query = "http://www.wyden.senate.gov/styles/global.css?v=2&amp;";
    System.out.println(query.replaceAll("&amp;", "&"));
    
    //final String input = "Tĥïŝ ĩš â fůňķŷ Šťŕĭńġ";
    String input = "http://www.mcconnell.senate.gov/";
    /*System.out.println(
        Normalizer
            .normalize(input, Normalizer.Form.NFD)
            .replaceAll("[^\\p{ASCII}]", "")
    );*/
    try {
      System.out.println(URLEncoder.encode(input, "US-ASCII"));
      input = URLEncoder.encode(input, "US-ASCII");
      System.out.println(URLEncoder.encode(input, "US-ASCII"));
    } catch (UnsupportedEncodingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
