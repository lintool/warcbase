package org.warcbase;

import org.eclipse.jetty.webapp.WebAppContext;

public class AppContextBuilder {
  
  private WebAppContext webAppContext;
  
  public WebAppContext buildWebAppContext(){
    webAppContext = new WebAppContext();
    //webAppContext.setDescriptor(webAppContext + "webapp/WEB-INF/web.xml");
    webAppContext.setDescriptor("src/main/webapp/WEB-INF/web.xml");
    System.out.println(webAppContext.getDescriptor());
    webAppContext.setResourceBase("src/main/webapp/index.html");
    webAppContext.setContextPath("/warcbase");
    return webAppContext;
  }
}
