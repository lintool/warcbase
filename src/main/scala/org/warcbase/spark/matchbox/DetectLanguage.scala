package org.warcbase.spark.matchbox

import org.apache.tika.language.LanguageIdentifier

object DetectLanguage {
  def apply(input: String) {
    if (input.isEmpty) Nil
    else new LanguageIdentifier(input).getLanguage
  }
}