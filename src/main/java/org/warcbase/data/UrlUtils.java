package org.warcbase.data;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class UrlUtils {
  private static final Joiner JOINER =  Joiner.on(".");

  public static String urlToKey(String in) {
    URL url = null;
    try {
      url = new URL(in);
    } catch (MalformedURLException mue) {
      return null;
    }
    String host = url.getHost();
    StringBuilder key = new StringBuilder();

    key.append(JOINER.join(Lists.reverse(Arrays.asList(host.split("\\.", 0)))));
    int port = url.getPort();
    if (port != -1) {
      key.append(":").append(port);
    }
    key.append(url.getFile());

    return key.toString();
  }

  public static String reverseHostname(String h) {
    String[] splits = h.split("\\/");
    String[] ports = splits[0].split("\\:", 0);

    StringBuilder host = new StringBuilder();
    host.append(JOINER.join(Lists.reverse(Arrays.asList(ports[0].split("\\.", 0)))));
    if (ports.length > 1) {
      host.append(":" + ports[1]);
    }

    return host.toString();
  }

  public static String keyToUrl(String reverse) {
    String domain = UrlUtils.getDomain(reverse);
    domain = UrlUtils.reverseHostname(domain);
    String[] splits = reverse.split("\\/");
    if (splits.length < 2) {
      return domain;
    }
    String file = reverse.substring(splits[0].length());
    return "http://" + domain + file;
  }

  // This method doesn't really make sense... should really be going with MIME types
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
}
