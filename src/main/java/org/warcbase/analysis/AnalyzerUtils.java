package org.warcbase.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class AnalyzerUtils {
  static public List<String> parse(Analyzer analyzer, String keywords) throws IOException {
    List<String> list = Lists.newArrayList();

    TokenStream tokenStream = analyzer.tokenStream(null, new StringReader(keywords));
    CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
    tokenStream.reset();
    org.apache.lucene.analysis.en.KStemFilter kstem = new KStemFilter(tokenStream);
    while (kstem.incrementToken()) {
      if (cattr.toString().length() == 0) {
        continue;
      }
      list.add(cattr.toString());
    }
    tokenStream.end();
    tokenStream.close();
    kstem.close();

    return list;
  }

  public static void main(String[] args) throws IOException {
    String text = "Media Center Press Releases: Congressman JohnCarter 31st District of Texas - Press Releases December 2003 Carter Comments on the Capture of Hussein  (December 14, 2003) Carter Congratulates Prairie View A&M on Grant  (December 9, 2003) Carter Acquires Funding for Brazos Valley  (December 8, 2003) arter Acquires Funding for Williamson County: Education, Transportation and Security  (December 8, 2003) November 2003 Fulfilling Our Promise to America�s Seniors: Carter Supports Historic Medicare Legislation  (November 22, 2003) Williamson County Bill Passes the House  (November 17, 2003) Carter to Host Congressional Field Hearing this Monday in Cypress  (November 6, 2003) President Signs the Partial Birth Abortion Ban  (November 5, 2003) October 2003 Rosie Babin Shares Her Son�s Story  (October 31, 2003) Williamson County Bill Set to Hit House Floor Soon  (October 29, 2003) Carter Announces Solution to Concurrent Receipt  (October 16, 2003) Katherine Harris Appears on Carter�s Monthly Show  (October 14, 2003) House Passes Partial Birth Abortion Conference Report  (October 2, 2003) September 2003 How to Do Business with Uncle Sam: Reps. Davis, Carter to Host Conference in Austin, Texas  (September 22, 2003) White House Tours to Resume  (September 10, 2003) President Urges Congress to Pass Carter�s Terrorist Bill  (September 10, 2003) House Approves Transportation Funding for Round Rock  (September 9, 2003) House approves over $2 million for Bryan/ College Station  (September 9, 2003) Carter Reminds Students about Military Academy Nominations  (September 9, 2003) Carter praises announcement from HUD  (September 5, 2003) August 2003 Carter to hold Town Halls  (August 19, 2003) Carter invites Chairman Mica to tour Easterwood Airport  (August 4, 2003) July 2003 Carter to hold Town Halls  (July 31, 2003) Carter Introduces Legislation  (July 31, 2003) Carter Introduces his Monthly Television Show  (July 31, 2003) Money back in Taxpayer Hands  (July 25, 2003) Babin Awarded Bronze Star Medal with Valor  (July 18, 2003) Williamson County named Primary Natural Disaster Area  (July 17, 2003) June 2003 Carter";
    // String text =
    // "announce fund federal department fire grant bun funds million county program senator community city project release press housing development";
    List<String> words = AnalyzerUtils.parse(new SimpleAnalyzer(Version.LUCENE_45), text);
    /*
     * StringTokenizer st = new StringTokenizer(text); List<String> words = Lists.newArrayList();
     * while (st.hasMoreElements()) { words.add((String) st.nextElement()); }
     */
    System.out.println(words.toString());
    ArrayList<String> stop_words = new ArrayList<String>();
    // stop_words.add("john");
    // stop_words.add("carter");
    words.removeAll(stop_words);
    System.out.println(Joiner.on(" ").join(words));
  }
}