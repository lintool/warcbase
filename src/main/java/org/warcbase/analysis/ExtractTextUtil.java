package org.warcbase.analysis;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.document.TextDocument;
import de.l3s.boilerpipe.extractors.ArticleExtractor;
import de.l3s.boilerpipe.sax.BoilerpipeSAXInput;
import de.l3s.boilerpipe.sax.HTMLDocument;

public class ExtractTextUtil {

  /**
   * @param args
   * @throws BoilerpipeProcessingException
   */
  public static void main(String[] args) throws BoilerpipeProcessingException {

  }

  public static String getText(String rawHtml) {
    try {
      TextDocument td = extractTextWithBoiler(preprocessWithJsoup(rawHtml));
      return td.getTitle() + " " + ArticleExtractor.INSTANCE.getText(td);
    } catch (BoilerpipeProcessingException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return "";
  }

  /**
   * 
   * @param file
   * @return
   */
  public static TextDocument extractTextWithBoiler(String rawHtml) {
    try {
      HTMLDocument htmlDoc = new HTMLDocument(rawHtml);
      InputSource inputSource = htmlDoc.toInputSource();
      BoilerpipeSAXInput boilerpipeSaxInput = new BoilerpipeSAXInput(inputSource);
      return boilerpipeSaxInput.getTextDocument();
    } catch (BoilerpipeProcessingException e) {
      e.printStackTrace();
    } catch (SAXException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return null;
  }

  /**
   * 
   * @param file
   * @return
   */
  public static String preprocessWithJsoup(String rawHtml) {
    Document doc;
    StringBuffer buf = new StringBuffer();
    try {
      doc = Jsoup.parse(rawHtml);

      // get title
      buf.append("<html>");
      buf.append("<head><title>" + doc.title() + "</title></head>");
      buf.append("<body>");
      // get certain level of accurate on content, by checking either
      // content id/content class

      Element content = doc.getElementById("content");
      if (content != null && content.hasText()) {
//        System.out.println("HAS ID CONTENT");
        buf.append(content.toString());
      } else {
        Elements elements = doc.getElementsByClass("content");
        if (elements != null && elements.hasText()) {
//          System.out.println("HAS CLASS CONTENT");
          for (Element element : elements) {
            buf.append(element.toString());
          }
        } else {
          elements = doc.getElementsByTag("p");
          if (elements != null && elements.hasText()) {
//            System.out.println("HAS P TAG");
            for (Element element : elements) {
              buf.append(element.toString());
            }
          } else {
//            System.out.println("HAS NONE OF THE ABOVE");
            return doc.html();
          }
        }
      }

      buf.append("</body>");
      buf.append("</html>");

    } catch (Exception e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    return buf.toString();
  }

}