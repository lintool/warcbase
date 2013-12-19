package org.warcbase.pig.piggybank;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.tika.Tika;
import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.parser.AutoDetectParser;
import org.opf_labs.LibmagicJnaWrapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class DetectMimeTypeMagic extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        String mimeType = "N/A";

        if (input == null || input.size() == 0 || input.get(0) == null) {
            return null;
        }
        String content = (String) input.get(0);

        InputStream is = new ByteArrayInputStream(content.getBytes());
        if (content.isEmpty()) return "EMPTY";

        LibmagicJnaWrapper jnaWrapper = new LibmagicJnaWrapper();
        jnaWrapper.load("/usr/local/Cellar/libmagic/5.15/share/misc/magic.mgc"); // Mac OS X with Homebrew
        //jnaWrapper.load("/usr/share/file/magic.mgc"); // CentOS

        mimeType = jnaWrapper.getMimeType(is);

        return mimeType;
    }
}
