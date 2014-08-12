package org.warcbase.browser;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class WarcBrowser {
  private static final Logger LOG = Logger.getLogger(WarcBrowser.class);

  private final Server server;

  public WarcBrowser(int runningPort) throws Exception {
    server = new Server(runningPort);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath("/");
    server.setHandler(context);
    context.addServlet(new ServletHolder(new WarcBrowserServlet()), "/*");

    ServletHolder holder = context.addServlet(DefaultServlet.class, "/warcbase/*");
    holder.setInitParameter("resourceBase", "src/main/webapp/");
    holder.setInitParameter("pathInfoOnly", "true");
  }

  public void start() throws Exception {
    server.start();
  }

  public void stop() throws Exception {
    server.stop();
    server.join();
  }

  public boolean isStarted() {
    return server.isStarted();
  }

  public boolean isStopped() {
    return server.isStopped();
  }

  private static final String PORT_OPTION = "port";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("num")
        .hasArg().withDescription("port to serve on").create(PORT_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(WarcBrowser.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(PORT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(WarcBrowser.class.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    int port = Integer.parseInt(cmdline.getOptionValue(PORT_OPTION));

    LOG.info("Starting server on port " + port);
    LOG.setLevel(Level.OFF);
    WarcBrowser browser = new WarcBrowser(port);

    browser.start();
  }
}
