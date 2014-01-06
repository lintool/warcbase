package org.warcbase.pig.piggybank;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.tika.language.LanguageIdentifier;

import java.io.IOException;

public class DetectLanguage extends EvalFunc<String> {
    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }
        String text = (String) input.get(0);
        return new LanguageIdentifier(text).getLanguage();
    }
}