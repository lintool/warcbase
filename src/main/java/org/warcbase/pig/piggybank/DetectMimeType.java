package org.warcbase.pig.piggybank;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import org.apache.tika.Tika;
import org.apache.tika.detect.DefaultDetector;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;

import org.apache.tika.parser.AutoDetectParser;

public class DetectMimeType extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }
        String content = (String) input.get(0);

        InputStream is = new ByteArrayInputStream( content.getBytes() );
        DefaultDetector detector = new DefaultDetector();
        AutoDetectParser parser = new AutoDetectParser(detector);
        return new Tika(detector, parser).detect(is);
    }
}
