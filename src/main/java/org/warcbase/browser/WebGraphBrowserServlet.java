package org.warcbase.browser;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.NavigableMap;
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
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.archive.util.ArchiveUtils;
import org.warcbase.data.HBaseTableManager;
import org.warcbase.data.UrlMapping;
import org.warcbase.data.UrlUtils;

public class WebGraphBrowserServlet extends HttpServlet {
  private static final long serialVersionUID = 847405540723915805L;
  private static final Logger LOG = Logger.getLogger(WebGraphBrowserServlet.class);
  
  private UrlMapping fst;
  private final Configuration hbaseConfig;
  private HBaseAdmin hbaseAdmin;
  private HConnection hbaseConnection;
  
  private final String TABLE_NAME = "extract-links";
  private final String COLUMN_FAMILY = "links";
  private final int DEFAULT_ID = 1; // default url id
  private final DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

  public WebGraphBrowserServlet(String urlMappingPath) throws 
    IOException, MasterNotRunningException, ZooKeeperConnectionException {
    fst = new UrlMapping(urlMappingPath);
    this.hbaseConfig = HBaseConfiguration.create();
    hbaseAdmin = new HBaseAdmin(hbaseConfig);
    hbaseConnection = HConnectionManager.createConnection(hbaseConfig);
  }

  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    String path = req.getPathInfo();
    String inputUrl = req.getParameter("url");
    String inputId = req.getParameter("id");
    
    String url = null;
    if (inputUrl == null) {
      if (inputId == null) {
        url = fst.getUrl(DEFAULT_ID);
      } else {
        url = fst.getUrl(Integer.parseInt(inputId));
      }
    } else {
      url = inputUrl;
    }
    LOG.info("requested " + url);
    
    writeContents(url, path, resp);
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
  
  public void writeContents(String url, String path, HttpServletResponse resp)
      throws IOException {
    String key = UrlUtils.urlToKey(url);
    LOG.info("key:" + key);
    HTableInterface table = hbaseConnection.getTable(TABLE_NAME);
    
    Get get = new Get(Bytes.toBytes(key));
    get.addFamily(Bytes.toBytes(COLUMN_FAMILY));
    Result rs = table.get(get);
    LOG.info("result size:" + rs.size());
    
    resp.setContentType("text/html");
    PrintWriter out = resp.getWriter();
    out.println("<html><body>");
    out.println("<b> Url: </b>" + url);
    
    if (rs == null || rs.size() == 0) {
      out.println("<p> can't find url in hbase table </p>");
      out.println("</body></html>");
      return;
    }
    
    NavigableMap<byte[], byte[]> familyMap = rs.getFamilyMap(Bytes.toBytes(COLUMN_FAMILY));
    for(byte[] column : familyMap.keySet()) {
      byte[] value = familyMap.get(column);
      Long epoch = Bytes.toLong(column);
      String time = df.format(new Date(epoch/1000));
      String targetIds = Bytes.toString(value);
      out.println("<br><b> Time: </b>" + time +"</br>");
      out.println("<ol>");
      for (String targetId : targetIds.split(",")) {
        try {
          String targetUrl = fst.getUrl(Integer.parseInt(targetId));
          String completeUrl = path + "?url=" + targetUrl;
          out.println("<li> <a href=" + completeUrl + ">"
              + targetUrl + "</a> </li>");
        } catch(Exception e) {
          continue;
        }
      }
      out.println("</ol>");
    }
    out.println("</body></html>");
  }
  
}
