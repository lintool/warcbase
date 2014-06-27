package org.warcbase.data;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.archive.wayback.ResultURIConverter;
import org.archive.wayback.replay.html.ReplayParseContext;
import org.archive.wayback.util.url.UrlOperations;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;


public class JSInternalUriConverter {
  private static final String EMAIL_PROTOCOL_PREFIX = "mailto:";
  private static final String JAVASCRIPT_PROTOCOL_PREFIX = "javascript:";
  private String tableName;
  private static String QUOTED_ATTR_VALUE = "(?:\"[^\">]*\")";
  private static String ESC_QUOTED_ATTR_VALUE = "(?:\\\\\"[^>\\\\]*\\\\\")";
  private static String APOSED_ATTR_VALUE = "(?:'[^'>]*')";
  private static String RAW_ATTR_VALUE = "(?:[^ \\t\\n\\x0B\\f\\r>\"']+)";
  private static int MIN_ATTR_LENGTH = 3;

  private static String ANY_ATTR_VALUE = QUOTED_ATTR_VALUE + "|"
      + APOSED_ATTR_VALUE + "|" + ESC_QUOTED_ATTR_VALUE + "|"
      + RAW_ATTR_VALUE;
  
  public JSInternalUriConverter(String tableName) {
    this.tableName = tableName;
  }
  
  public String makeReplayURI(String datespec, String url) {
    //System.out.println("Salam, url = "+ url);
    if (url.startsWith(EMAIL_PROTOCOL_PREFIX)) {
      return url;
    }
    if (url.startsWith(JAVASCRIPT_PROTOCOL_PREFIX)) {
      return url;
    }
    StringBuilder sb = null;
    String replayURIPrefix = null;
    if (replayURIPrefix == null) {
      sb = new StringBuilder(url.length() + datespec.length());
      sb.append(TextDocument2.SERVER_PREFIX + tableName + "/");
      sb.append("nobanner" + "/");
      sb.append(datespec);
      sb.append("/");
      sb.append(url);
      String newUrl = sb.toString();
      newUrl = newUrl.replaceAll("%22", "\"");
      newUrl = newUrl.replaceAll("%5B", "[");
      newUrl = newUrl.replaceAll("%5D", "]");
      newUrl = newUrl.replaceAll("%2A", "*");
      newUrl = newUrl.replaceAll("%2B", "+");
      newUrl = newUrl.replaceAll("%2C", ",");
      newUrl = newUrl.replaceAll("%2D", "-");
      newUrl = newUrl.replaceAll("%2E", ".");
      newUrl = newUrl.replaceAll("%2F", "/");
      newUrl = newUrl.replaceAll("%3A", ":");
      newUrl = newUrl.replaceAll("%3B", ";");
      newUrl = newUrl.replaceAll("%3D", "=");
      return newUrl;
    }
    if (url.startsWith(replayURIPrefix)) {
      return url;
    }
    sb = new StringBuilder(url.length() + datespec.length());
    sb.append(replayURIPrefix);
    sb.append(datespec);
    sb.append("/");
    sb.append(UrlOperations.stripDefaultPortFromUrl(url));
    return sb.toString();
  }
  
  public String fixURLs(String content, String pageUrl, String captureDate, String tableName) {
    Document doc = Jsoup.parse(content);
    String baseUrl = doc.baseUri();
    Elements scripts = doc.getElementsByTag("script");
    String attrName = "location";
    String tagPatString = attrName
        + "\\s*=\\s*(" + ANY_ATTR_VALUE + ")(?:\\s|>)?";
    Pattern pattern = Pattern.compile(tagPatString, Pattern.CASE_INSENSITIVE);
    for (Element script: scripts) {
      //System.out.println(script.html());
      StringBuilder scriptBody = new StringBuilder(script.html());
      Matcher matcher = pattern.matcher(scriptBody);
      int idx = 0;
      while (matcher.find(idx)) {
        String url = matcher.group(1);
        //System.out.println(url);
        int origUrlLength = url.length();
        int attrStart = matcher.start(1);
        int attrEnd = matcher.end(1);
        if(origUrlLength < MIN_ATTR_LENGTH) {
          idx = attrEnd;
          continue;
        }
        String quote = "";
        if (url.charAt(0) == '"') {
          quote = "\"";
          url = url.substring(1, origUrlLength - 1);
        } else if (url.charAt(0) == '\'') {
          quote = "'";
          url = url.substring(1, origUrlLength - 1);
        } else if (url.charAt(0) == '\\') {
          quote = "\\\"";
          url = url.substring(2, origUrlLength - 2);
        }
        if ((url.charAt(0) == '\'' || url.charAt(0) == '"') && url.length() <= MIN_ATTR_LENGTH) {
          idx = attrEnd;
          continue;
        }
        if (url.startsWith(ReplayParseContext.DATA_PREFIX)) {
          idx = attrEnd;
          continue;
        }
        String finalUrl = UrlOperations.resolveUrl(baseUrl,url);
        String replayUrl = quote
            + new JSInternalUriConverter("c108th").makeReplayURI("100000", finalUrl) + quote;
        //System.out.println(finalUrl + " " + replayUrl);
        int delta = replayUrl.length() - origUrlLength;
        scriptBody.replace(attrStart, attrEnd, replayUrl);
        idx = attrEnd + delta;
      }
      script.text(scriptBody.toString());
      //output += scriptBody;
    }
    return doc.html();
    
  }
  
  public static void main(String[] args) throws IOException {
    Document doc = Jsoup.connect("http://tibanna.umiacs.umd.edu:9191/c108th/1072317789000/http://daschle.senate.gov/").get();
    //String existingBaseHref = TagMagix2.getBaseHref(new StringBuilder(doc.html()));
    String baseUrl = doc.baseUri();
    Elements scripts = doc.getElementsByTag("script");
    String attrName = "location";
    String tagPatString = attrName
        + "\\s*=\\s*(" + ANY_ATTR_VALUE + ")(?:\\s|>)?";
    Pattern pattern = Pattern.compile(tagPatString, Pattern.CASE_INSENSITIVE);
    String output = "";
    for (Element script: scripts) {
      //System.out.println(script.html());
      StringBuilder scriptBody = new StringBuilder(script.html());
      Matcher matcher = pattern.matcher(scriptBody);
      int idx = 0;
      while (matcher.find(idx)) {
        String url = matcher.group(1);
        //System.out.println(url);
        int origUrlLength = url.length();
        int attrStart = matcher.start(1);
        int attrEnd = matcher.end(1);
        if(origUrlLength < MIN_ATTR_LENGTH) {
          idx = attrEnd;
          continue;
        }
        String quote = "";
        if (url.charAt(0) == '"') {
          quote = "\"";
          url = url.substring(1, origUrlLength - 1);
        } else if (url.charAt(0) == '\'') {
          quote = "'";
          url = url.substring(1, origUrlLength - 1);
        } else if (url.charAt(0) == '\\') {
          quote = "\\\"";
          url = url.substring(2, origUrlLength - 2);
        }
        if ((url.charAt(0) == '\'' || url.charAt(0) == '"') && url.length() <= MIN_ATTR_LENGTH) {
          idx = attrEnd;
          continue;
        }
        if (url.startsWith(ReplayParseContext.DATA_PREFIX)) {
          idx = attrEnd;
          continue;
        }
        String finalUrl = UrlOperations.resolveUrl(baseUrl,url);
        String replayUrl = quote
            + new JSInternalUriConverter("c108th").makeReplayURI("100000", finalUrl) + quote;
        //System.out.println(finalUrl + " " + replayUrl);
        int delta = replayUrl.length() - origUrlLength;
        scriptBody.replace(attrStart, attrEnd, replayUrl);
        idx = attrEnd + delta;
      }
      script.text(scriptBody.toString());
      //output += scriptBody;
    }
    
    //System.out.println(output);
    System.out.println(doc.html());
  }
}
