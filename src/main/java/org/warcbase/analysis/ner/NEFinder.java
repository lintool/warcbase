package org.warcbase.analysis.ner;

import java.util.List;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;

public class NEFinder {
  private AbstractSequenceClassifier<CoreLabel> classifier = null;

  public NEFinder(String serializedClassifier) {
    classifier = CRFClassifier.getClassifierNoExceptions(serializedClassifier);
  }

  public String replaceNER(String text) {
    if (classifier == null) {
      System.out.println("please init classifier");
      return text;
    }
    StringBuilder sb = new StringBuilder("");
    List<List<CoreLabel>> out = classifier.classify(text);
    Boolean prevNER = false;
    for (List<CoreLabel> sentence : out) {
      for (CoreLabel word : sentence) {
        if (word.get(CoreAnnotations.AnswerAnnotation.class).equals("O")) {
          if (prevNER) {
            prevNER = false;
            sb.append(' ');
          }
          sb.append(word.word() + ' ');
        } else {
          if (prevNER) {
            // sb.append('_');
          }
          prevNER = true;
          sb.append(word.word());
        }
      }
    }
    return new String(sb);
  }
}
