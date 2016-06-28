/*
 * Warcbase: an open-source platform for managing web archives
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.warcbase.data;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.Arc;
import org.apache.lucene.util.fst.FST.BytesReader;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;
import org.warcbase.ingest.IngestFiles;

public class UrlMapping {
  private static final Logger LOG = Logger.getLogger(UrlMapping.class);

  private FST<Long> fst;

  public UrlMapping(FST<Long> fst) {
    this.fst = fst;
  }

  public UrlMapping() {
  }

  public UrlMapping(String outputFileName) {
    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    File outputFile = new File(outputFileName);
    try {
      this.fst = FST.read(outputFile, outputs);
    } catch (IOException e) {
      LOG.error("Build FST Failed!");
      e.printStackTrace();
    }
  }

  public void loadMapping(String outputFileName) {
    UrlMapping tmp = new UrlMapping(outputFileName);
    this.fst = tmp.fst;
  }

  public FST<Long> getFst() {
    return fst;
  }

  public int getID(String url) {
    Long id = null;
    try {
      id = Util.get(fst, new BytesRef(url));
    } catch (IOException e) {
      // Log error, but assume that URL doesn't exist.
      LOG.error("Error fetching " + url);
      e.printStackTrace();
      return -1;
    }

    return id == null ? -1 : id.intValue();
  }

  public String getUrl(int id) {
    BytesRef scratchBytes = new BytesRef();
    IntsRef key = null;

    try {
      key = Util.getByOutput(fst, id);
    } catch (IOException e) {
      LOG.error("Error id " + id);
      e.printStackTrace();
      return null;
    }

    if (key == null) {
      return null;
    }
    return Util.toBytesRef(key, scratchBytes).utf8ToString();
  }

  public List<String> prefixSearch(String prefix) {
    if (prefix == null || prefix.length() == 0 ) {
      return new ArrayList<String>();
    }

    List<String> strResults = null;
    try {
      // descend to the arc of the prefix string
      Arc<Long> arc = fst.getFirstArc(new Arc<Long>());
      BytesReader fstReader = fst.getBytesReader();
      BytesRef bref = new BytesRef(prefix);
      for (int i = 0; i < bref.length; i++) {
        Arc<Long> retArc = fst.findTargetArc(bref.bytes[i + bref.offset] & 0xFF, arc, arc, fstReader);
        if (retArc == null) { // no matched prefix
          return new ArrayList<String>();
        }
      }

      // collect all substrings started from the arc of prefix string.
      List<BytesRef> result = new ArrayList<BytesRef>();
      BytesRef newPrefixBref = new BytesRef(prefix.substring(0, prefix.length() - 1));
      collect(result, fstReader, newPrefixBref, arc);

      // convert BytesRef results to String results
      strResults = new ArrayList<String>();
      Iterator<BytesRef> iter = result.iterator();
      while (iter.hasNext()) {
        strResults.add(iter.next().utf8ToString());
      }
    } catch (IOException e) {
      LOG.error("Error: " + e);
      e.printStackTrace();
      return new ArrayList<String>();
    }

    return strResults;
  }

  public int[] getIdRange(String first, String last){
    if (first == null || last == null) {
      return null;
    }

    Long startId = null, endId = null;
    try {
      startId = Util.get(fst, new BytesRef(first));
      endId = Util.get(fst, new BytesRef(last));

      if (startId == null || endId == null) {
        return null;
      }
    } catch (IOException e) {
      LOG.error("Error: " + e);
      e.printStackTrace();
      return null;
    }

    return new int[] { (int) startId.longValue(), (int) endId.longValue() };
  }

  private boolean collect(List<BytesRef> res, BytesReader fstReader,
      BytesRef output, Arc<Long> arc) throws IOException {
    if (output.length == output.bytes.length) {
      output.bytes = ArrayUtil.grow(output.bytes);
    }
    assert output.offset == 0;
    output.bytes[output.length++] = (byte) arc.label;

    fst.readFirstTargetArc(arc, arc, fstReader);
    while (true) {
      if (arc.label == FST.END_LABEL) {
        res.add(BytesRef.deepCopyOf(output));
      } else {
        int save = output.length;
        if (collect(res, fstReader, output, new Arc<Long>().copyFrom(arc))) {
          return true;
        }
        output.length = save;
      }

      if (arc.isLast()) {
        break;
      }
      fst.readNextArc(arc, fstReader);
    }
    return false;
  }

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws Exception {
    final String DATA = "data";
    final String ID = "getId";
    final String URL = "getUrl";
    final String PREFIX = "getPrefix";

    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("FST data file").create(DATA));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("get id").create(ID));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("get url").create(URL));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("get prefix").create(PREFIX));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: "
          + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(DATA) || (!cmdline.hasOption(ID)
        && !cmdline.hasOption(URL) && !cmdline.hasOption(PREFIX))) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(IngestFiles.class.getCanonicalName(), options);
      System.exit(-1);
    }

    String filePath = cmdline.getOptionValue(DATA);
    UrlMapping map = new UrlMapping(filePath);
    map.loadMapping(filePath);

    if (cmdline.hasOption(ID)) {
      String url = cmdline.getOptionValue(ID);
      System.out.println(map.getID(url));
    }

    if (cmdline.hasOption(URL)) {
      int id = Integer.parseInt(cmdline.getOptionValue(URL));
      System.out.println(map.getUrl(id));
    }

    if (cmdline.hasOption(PREFIX)) {
      String prefix = cmdline.getOptionValue(PREFIX);
      List<String> urls = map.prefixSearch(prefix);
      for (String s : urls) {
        System.out.println(s);
      }
    }
  }
}
