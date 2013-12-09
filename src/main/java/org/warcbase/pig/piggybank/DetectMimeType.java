package org.warcbase.pig.piggybank;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import org.apache.tika.Tika;
import org.apache.tika.detect.DefaultDetector;

import org.opf_labs.LibmagicJnaWrapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;

import org.apache.tika.parser.AutoDetectParser;

public class DetectMimeType extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        String mimeType = "N/A";

        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }
        String content = (String) input.get(0);
        String engine = (String) input.get(1);
        if (!engine.equals("magic") && !engine.equals("tika"))
            return "UNKNOWN ENGINE";

        InputStream is = new ByteArrayInputStream(content.getBytes());
        if (content.isEmpty()) return "EMPTY";

        if (engine.equals("magic")) {

            LibmagicJnaWrapper jnaWrapper = new LibmagicJnaWrapper();
            jnaWrapper.load("/usr/local/Cellar/libmagic/5.15/share/misc/magic.mgc");

            mimeType = jnaWrapper.getMimeType(is);

        } else if (engine.equals("tika")) {

            DefaultDetector detector = new DefaultDetector();
            AutoDetectParser parser = new AutoDetectParser(detector);
            mimeType =  new Tika(detector, parser).detect(is);
            //return new Tika(detector, parser).detect(is);
        }
        return mimeType;
    }
}
