package org.warcbase.pig.piggybank;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.tika.Tika;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.parser.AutoDetectParser;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DetectMimeTypeTika extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        String mimeType;

        if (input == null || input.size() == 0 || input.get(0) == null) {
            return "N/A";
        }
        String content = (String) input.get(0);

        InputStream is = new ByteArrayInputStream(content.getBytes());
        if (content.isEmpty()) return "EMPTY";

        DefaultDetector detector = new DefaultDetector();
        AutoDetectParser parser = new AutoDetectParser(detector);
        mimeType =  new Tika(detector, parser).detect(is);

        return mimeType;
    }
}
