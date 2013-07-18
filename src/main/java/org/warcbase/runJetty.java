package org.warcbase;

//import javax.servlet.HttpConstraintElement;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.webapp.WebAppContext;

public class runJetty {

   public static void main(String[] args) {
     //WebAppContext webAppContext = new WebAppContext();
     //System.out.println(webAppContext);
   //  HttpConstraintElement h = null;
     JettyServer jettyServer = new JettyServer();
     ContextHandlerCollection contexts = new ContextHandlerCollection();
     
     contexts.setHandlers(new Handler[] 
       { new AppContextBuilder().buildWebAppContext()});
     
     jettyServer.setHandler(contexts);
     
     try {
      jettyServer.start();
    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
     
  }
}
