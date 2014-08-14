package org.warcbase.pig.piggybank;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

// http://www.openplanetsfoundation.org/blogs/2014-01-23-standing-shoulders-your-peers
public class DetectMimeTypeMagic extends EvalFunc<String> {

    @Override
    public String exec(Tuple input) throws IOException {
        String mimeType = null;

        if (input == null || input.size() == 0 || input.get(0) == null) {
            return "N/A";
        }
        String magicFile = (String) input.get(0);
        String content = (String) input.get(1);

        InputStream is = new ByteArrayInputStream(content.getBytes());
        if (content.isEmpty()) return "EMPTY";

        // I'm commenting this out because the jar isn't actually published anywhere...
        // @lintool 2014/08/12

        //org.opf_labs.LibmagicJnaWrapper jnaWrapper = new LibmagicJnaWrapper();
        //jnaWrapper.load(magicFile);

        //jnaWrapper.load("/usr/local/Cellar/libmagic/5.15/share/misc/magic.mgc"); // Mac OS X with Homebrew
        //jnaWrapper.load("/usr/share/file/magic.mgc"); // CentOS

        //mimeType = jnaWrapper.getMimeType(is);
        return mimeType;
    }
}
