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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FST.INPUT_TYPE;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

public class UrlMappingBuilder {
  private static final Logger LOG = Logger.getLogger(UrlMappingBuilder.class);

  private static void readUrlFromFile(File f, List<String> urls) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(f));
    String line;
    while ((line = br.readLine()) != null) {
      if (!line.equals("")) {
        String url = line.split("\\s+")[0];
        urls.add(url);
      }
    }
    LOG.info("Read " + f + ", " + urls.size() + " URLs");
    br.close();
  }

  private static List<String> readUrlFromFolder(String folderName) throws IOException {
    File folder = new File(folderName);
    List<String> urls = new ArrayList<String>();
    if (folder.isDirectory()) {
      for (File file : folder.listFiles()) {
        readUrlFromFile(file, urls);
      }
    } else {
      readUrlFromFile(folder, urls);
    }

    LOG.info("Sorting URLs...");
    Collections.sort(urls); // sort URLs alphabetically
    LOG.info("Done sorting!");

    return urls;
  }

  public static final String INPUT_OPTION = "input";
  public static final String OUTPUT_OPTION = "output";

  @SuppressWarnings("static-access")
  public static void main(String[] args) throws IOException {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT_OPTION));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT_OPTION));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();
    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(UrlMappingBuilder.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INPUT_OPTION) || !cmdline.hasOption(OUTPUT_OPTION)) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(UrlMappingBuilder.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String input = cmdline.getOptionValue(INPUT_OPTION);
    String output = cmdline.getOptionValue(OUTPUT_OPTION);

    List<String> inputValues = null;
    try {
      inputValues = readUrlFromFolder(input); // read data
    } catch (IOException e) {
      e.printStackTrace();
    }

    PositiveIntOutputs outputs = PositiveIntOutputs.getSingleton();
    Builder<Long> builder = new Builder<Long>(INPUT_TYPE.BYTE1, outputs);
    BytesRef scratchBytes = new BytesRef();
    IntsRef scratchInts = new IntsRef();
    for (int i = 0; i < inputValues.size(); i++) {
      if (i % 100000 == 0) {
        LOG.info(i + " URLs processed.");
      }
      scratchBytes.copyChars((String) inputValues.get(i));
      try {
        builder.add(Util.toIntsRef(scratchBytes, scratchInts), (long) i);
      } catch (UnsupportedOperationException e) {
        System.out.println("Duplicate URL:" + inputValues.get(i));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    FST<Long> fst = builder.finish();

    // Save FST to file
    File outputFile = new File(output);
    fst.save(outputFile);
    LOG.info("Wrote output to " + output);
  }
}
