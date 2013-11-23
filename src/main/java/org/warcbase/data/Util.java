package org.warcbase.data;

import java.net.MalformedURLException;
import java.net.URL;

public class Util {
  public static String reverseHostname(String uri) {
    URL url = null;
    try {
      url = new URL(uri);
    } catch (MalformedURLException mue) {
      return null;
    }
    String host = url.getHost();
    StringBuilder newhost = new StringBuilder();
    String[] parts = host.split("\\.", 0);
    for (int i = parts.length - 1; i > 0; i--) {
      if (i > 0)
        newhost.append(parts[i]).append(".");
    }
    newhost.append(parts[0]);
    int port = url.getPort();
    if (port != -1)
      newhost.append(":").append(port);
    newhost.append(url.getFile());
    return newhost.toString();
  }

  public static String reverseBacHostnamek(String reverse) {
    String[] splits = reverse.split("\\/");
    String[] parts = splits[0].split("\\.", 0);
    StringBuilder newhost = new StringBuilder();
    newhost.append("http://");
    String[] ports = splits[0].split("\\:", 0);
    String port = null;
    if (ports.length > 1)
      port = ports[1];
    parts = ports[0].split("\\.", 0);
    for (int i = parts.length - 1; i > 0; i--) {
      if (i > 0)
        newhost.append(parts[i]).append(".");
    }
    newhost.append(parts[0]);
    if (port != null)
      newhost.append(":" + port);
    return newhost.toString();
  }

  public static String getFileType(String url) {
    if (url.length() > 0 && url.charAt(url.length() - 1) == '/')
      return "";
    String[] splits = url.split("\\/");
    if (splits.length == 0)
      return "";
    splits = splits[splits.length - 1].split("\\.");
    if (splits.length <= 1)
      return "";
    String type = splits[splits.length - 1];
    if (type.length() > 8)
      return "";
    if (type.length() == 1 && Character.isDigit(type.charAt(0)))
      return "";
    return type;
  }

  public static String getDomain(String url) {
    String[] splits = url.split("\\/");
    return splits[0];
  }

  public static void main(String[] args) {
    System.out.println(Util.reverseHostname("http://www.boxer.senate.gov/"));// http://www.ayotte.senate.gov/
    System.out.println(Util
        .reverseHostname("https://tibanna.umiacs.umd.edu:9191/warc_congress108/all"));
    String host = Util.reverseHostname("https://tibanna.umiacs.umd.edu/warc_congress108/all");
    String[] splits = host.split("\\/");
    String[] parts = splits[0].split("\\.", 0);
    StringBuilder newhost = new StringBuilder();
    newhost.append("http://");
    String[] ports = splits[0].split("\\:", 0);
    String port = null;
    if (ports.length > 1)
      port = ports[1];
    parts = ports[0].split("\\.", 0);
    if (parts.length - 1 > 0) {

      System.out.println(ports.length);
      System.out.println(ports[0]);
      // if(ports.length > 1)
      // newhost.append(ports[0]);

      // else
      // newhost.app
    }
    for (int i = parts.length - 1; i > 0; i--) {
      if (i > 0)
        newhost.append(parts[i]).append(".");
    }
    newhost.append(parts[0]);
    if (port != null)
      newhost.append(":" + port);
    System.out.println(Util.reverseBacHostnamek(host));
  }

  public static String getUriExtension(String thisTargetURI) {
    if (thisTargetURI.length() > 3) {
      return thisTargetURI.substring(thisTargetURI.length() - 3, thisTargetURI.length());
    }

    return "";
  }

  public static String urlTRansform(String q, String date) {
    return null;
  }

}
