package org.warcbase.spark.matchbox

import org.apache.tika.language.LanguageIdentifier

object DetectLanguage {
  def apply(input: String): String = {
    if (input.isEmpty) ""
    else new LanguageIdentifier(input).getLanguage
  }
}