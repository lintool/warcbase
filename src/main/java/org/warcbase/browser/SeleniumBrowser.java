package org.warcbase.browser;

import java.util.List;
import java.util.Random;

import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.firefox.FirefoxDriver;

import com.google.common.collect.Lists;

public class SeleniumBrowser {
  private static final String[] jumpTargets = new String[] {
    //"http://localhost:9090/wayback/*/http://www.house.gov/",
    //"http://localhost:9090/wayback/*/http://www.senate.gov/",
    "http://localhost:9090/wayback/*/http://www.house.gov/house/MemberWWW.html",
    "http://localhost:9090/wayback/*/http://www.house.gov/house/CommitteeWWW.html",
    "http://localhost:9090/wayback/*/http://www.senate.gov/general/contact_information/senators_cfm.cfm",
    "http://localhost:9090/wayback/*/http://www.senate.gov/pagelayout/committees/d_three_sections_with_teasers/committees_home.htm"
  };

  public static void main(String[] args) throws InterruptedException {
    WebDriver driver = new FirefoxDriver();
    Random r = new Random(System.currentTimeMillis());

    driver.get("http://localhost:9090/wayback/*/http://www.house.gov/");

    for (int i = 0; i < 1000; i++) {
      List<WebElement> links = driver.findElements(By.tagName("a"));
      List<String> candidates = Lists.newArrayList();
      for (WebElement myElement : links) {
        String href = myElement.getAttribute("href");
        if (href != null && href.matches("^http://localhost:9090/wayback/\\d+.*$")) {
          candidates.add(href);
        }
      }

      if (candidates.size() < 3 ) {
        driver.navigate().back();
      } else if (r.nextFloat() < 0.1f) {
        String target = jumpTargets[r.nextInt(jumpTargets.length)];
        System.out.println("Jumping to " + target);
        driver.get(target);
      } else {
        String target = candidates.get(r.nextInt(candidates.size()));
        System.out.println("Navigating to " + target);
        driver.get(target);
      }
    }
    
    driver.quit();
  }
}