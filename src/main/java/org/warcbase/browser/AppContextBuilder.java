package org.warcbase.browser;

import org.eclipse.jetty.webapp.WebAppContext;

public class AppContextBuilder {

  private WebAppContext webAppContext;

  public WebAppContext buildWebAppContext() {
    webAppContext = new WebAppContext();
    webAppContext.setDescriptor("src/main/webapp/WEB-INF/web.xml");
    System.out.println(webAppContext.getDescriptor());
    webAppContext.setResourceBase("src/main/webapp/index.html");
    webAppContext.setContextPath("/warcbase");
    return webAppContext;
  }
}
