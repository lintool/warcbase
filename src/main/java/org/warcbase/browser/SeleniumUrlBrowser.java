package org.warcbase.browser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.firefox.FirefoxDriver;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;

public class SeleniumUrlBrowser {
  private static final Logger LOG = Logger.getLogger(SeleniumUrlBrowser.class);

  public static final String INPUT_OPTION = "input";

  @SuppressWarnings("static-access")
  public static void main(String[] args)
      throws InterruptedException, FileNotFoundException, IOException {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(SeleniumUrlBrowser.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(SeleniumUrlBrowser.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String file = cmdline.getOptionValue(INPUT_OPTION);

    WebDriver driver = new FirefoxDriver();
    // http://stackoverflow.com/questions/15122864/selenium-wait-until-document-is-ready
    ExpectedCondition<Boolean> pageLoadCondition = new
        ExpectedCondition<Boolean>() {
            public Boolean apply(WebDriver driver) {
                return ((JavascriptExecutor)driver).executeScript("return document.readyState").equals("complete");
            }
        };
    WebDriverWait wait = new WebDriverWait(driver, 30);

    BufferedReader br = new BufferedReader(new FileReader(file));
    String url;
    long begin, end;
    long total = 0;
    int cnt = 0;
    while ((url = br.readLine()) != null) {
      begin = System.currentTimeMillis();
      driver.get(url);
      wait.until(pageLoadCondition);
      end = System.currentTimeMillis();
      LOG.info(url + " loaded in " + (end-begin) + " ms");
      total += (end-begin);
      cnt++;
    }
    driver.quit();
    LOG.info(cnt + " URLS in " + total + " ms");
    LOG.info((total/cnt) + " ms per URL");
    br.close();
  }
}
