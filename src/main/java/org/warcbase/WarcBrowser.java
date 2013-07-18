package org.warcbase;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

public class WarcBrowser {
  private static final Logger LOG = Logger.getLogger(WarcBrowser.class);

  private Server server;

  public WarcBrowser() {
    this(8080);
  }

  public WarcBrowser(Integer runningPort) {
    server = new Server(runningPort);
  }

  public void setHandler(ContextHandlerCollection contexts) {
    server.setHandler(contexts);
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
  private static final String SERVER_OPTION = "server";

  @SuppressWarnings("static-access")
  public static void main(String[] args) {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("port to serve on").create(PORT_OPTION));
    options.addOption(OptionBuilder.withArgName("url").hasArg()
        .withDescription("server prefix").create(SERVER_OPTION));

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

    if (!cmdline.hasOption(PORT_OPTION) || !cmdline.hasOption(SERVER_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(WarcBrowser.class.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    int port = Integer.parseInt(cmdline.getOptionValue(PORT_OPTION));
    String server = cmdline.getOptionValue(SERVER_OPTION);

    LOG.info("Starting server on port " + port + " with server prefix " + server);
    WarcBrowser jettyServer = new WarcBrowser(port);
    TextDocument2.SERVER_PREFIX = server;
    ContextHandlerCollection contexts = new ContextHandlerCollection();

    contexts.setHandlers(new Handler[] { new AppContextBuilder().buildWebAppContext() });

    jettyServer.setHandler(contexts);

    try {
      jettyServer.start();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
