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

  public static void main(String[] args) {
    System.out.println(Util.reverseHostname("http://www.boxer.senate.gov/"));// http://www.ayotte.senate.gov/

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
