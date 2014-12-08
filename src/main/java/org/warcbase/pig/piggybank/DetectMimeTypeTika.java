package org.warcbase.pig.piggybank;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.tika.Tika;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.parser.AutoDetectParser;

public class DetectMimeTypeTika extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        String mimeType;

        if (input == null || input.size() == 0 || input.get(0) == null) {
            return "N/A";
        }
        DataByteArray content = (DataByteArray) input.get(0);

        InputStream is = new ByteArrayInputStream(content.get());

        DefaultDetector detector = new DefaultDetector();
        AutoDetectParser parser = new AutoDetectParser(detector);
        mimeType =  new Tika(detector, parser).detect(is);

        return mimeType;
    }
}
