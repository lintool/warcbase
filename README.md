Warcbase
========

Warcbase is an open-source platform for managing web archives built on Hadoop and HBase. The platform provides a flexible data model for storing and managing raw content as well as metadata and extracted knowledge. Tight integration with Hadoop provides powerful tools for analytics and data processing.


Getting Started
---------------

Clone the repo:

```
$ git clone git@github.com:lintool/warcbase.git
```

You can then build Warcbase:

```
$ mvn clean package appassembler:assemble
```

For the impatient, to skip tests:

```
$ mvn clean package appassembler:assemble -DskipTests
```

To create Eclipse project files:

```
$ mvn eclipse:clean
$ mvn eclipse:eclipse
```

You can then import the project into Eclipse.


Ingesting Content
-----------------

Somewhat ironically (given the name of the project), Warcbase currently supports only ARC files. Don't worry, we're [on it](https://github.com/lintool/warcbase/issues/64).

You can find some sample data [here](https://archive.org/details/ExampleArcAndWarcFiles). Ingesting data into Warcbase is fairly straightforward:

```
$ setenv CLASSPATH_PREFIX "/etc/hbase/conf/"
$ sh target/appassembler/bin/IngestFiles \
    -dir /path/to/warc/dir/ -name archive_name -create
```

Command-line options:

+ Use the `-dir` option to specify the directory containing the data files.
+ Use the `-name` option to specify the name of the archive (will correspond to the HBase table name).
+ Use the `-create` option to create a new table (and drop the existing table if a table with the same name exists already). Alternatively, use `-append` to add to an existing table.

That should do it. The data should now be in Warcbase.


Wayback/Warcbase Integration
----------------------------

Warcbase comes with a browser exposed as a REST API that conforms to Wayback's schema of `collection/YYYYMMDDHHMMSS/targetURL`. Here's how you start the browser:

```
$ setenv CLASSPATH_PREFIX "/etc/hbase/conf/"
$ sh target/appassembler/bin/WarcBrowser -port 8080
```

You can now use `http://myhost:8080/` to browse the archive. For example:

+ `http://myhost:8080/mycollection/*/http://mysite.com/` will give you a list of available versions of `http://mysite.com/`.
+ `http://myhost:8080/mycollection/19991231235959/http://mysite.com/` will give you the record of `http://mysite.com/` just before Y2K.

Note that this API serves up raw records, so the HTML pages don't look pretty, and images don't render properly (since the browser gets confused by record headers). So how do you actually navigate through the archive? This is where Wayback/Warcbase integration comes in.

As it turns out, the Wayback code has the ability separate rendering/browsing from data storage. More details can be found in this [technical overview](https://github.com/iipc/openwayback/wiki/Technical-overview). In short, we can customize a Wayback instance to point at the Warcbase REST API, and have the Wayback fetch records from HBase. This is accomplished by custom implementations of `ResourceIndex` and `ResourceStore` in [here](https://github.com/lintool/warcbase/tree/master/src/main/java/org/warcbase/wayback).

Here's how to install the integration:

1. Make sure you already have Wayback installed. See this [installation guide](https://github.com/iipc/openwayback/wiki/How-to-install) and [configuration guide](https://github.com/iipc/openwayback/wiki/How-to-configure).
2. Add the Warcbase jar to the Wayback's WAR deployment. In a standard setup, you would copy `warcbase-0.1.0-SNAPSHOT.jar` to the `TOMCAT_ROOT/webapps/ROOT/WEB-INF/lib/`.
3. Replace the `BDBCollection.xml` configuration in `TOMCAT_ROOT/webapps/ROOT/WEB-INF/` with the version in [`src/main/resources/`](https://github.com/lintool/warcbase/tree/master/src/main/resources).
4. Open up `BDBCollection.xml` and specify the correct `HOST`, `PORT`, and `TABLE`.
5. Shutdown and restart Tomcat.

Now navigate to your Wayback as before. Enjoy browsing your web archive!


Building the URL mapping
------------------------

It's convenient for a variety of tasks to map every URL to a unique integer id. Lucene's FST package provides a nice API for this task.

There are two ways to build the URL mapping, the first of which is via a MapReduce job:

```
$ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar \
    org.warcbase.data.UrlMappingMapReduceBuilder \
    -input /hdfs/path/to/data -output fst.dat
```

The FST data in this case will be written to HDFS. The potential issue with this approach is that building the FST is relatively memory hungry, and cluster memory is sometimes scarce.

The alternative is to build the mapping locally on a machine with sufficient memory. To do this, first run a MapReduce job to extract all the unique URLs:

```
$ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar \
    org.warcbase.analysis.ExtractUniqueUrls \
    -input /hdfs/path/to/data -output urls
```

Now copy the `urls/` directory out of HDFS and then run the following program:

```
$ sh target/appassembler/bin/UrlMappingBuilder -input urls -output fst.dat
```

Where `urls` is the output directory from above and `fst.dat` is the name of the FST data file. We can examine the FST data with the following utility program:

```
# Lookup by URL, fetches the integer id
$ sh target/appassembler/bin/UrlMapping -data fst.dat -getId http://www.foo.com/

# Lookup by id, fetches the URL
$ sh target/appassembler/bin/UrlMapping -data fst.dat -getUrl 42

# Fetches all URLs with the prefix
$ sh target/appassembler/bin/UrlMapping -data fst.dat -getPrefix http://www.foo.com/
```

Now copy the fst.dat file into HDFS for use in the next step:

```
$ hadoop fs -put fst.dat /hdfs/path/
```

Extracting the Webgraph
-----------------------

We can use the mapping data (from above) to extract the webgraph and at the same time map URLs to unique integer ids. This is accomplished by a Hadoop program:

```
$ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar \
    org.warcbase.analysis.graph.ExtractLinksWac \
    -hdfs /hdfs/path/to/data -output output -urlMapping fst.dat
```

Finally, instead of extracting links between individual URLs, we can extract the site-level webgraph by aggregating all URLs with common prefix into a "supernode". Link counts between supernodes represent the total number of links between individual URLs. In order to do this, following input files are needed:

+ a prefix file providing URL prefixes for each supernode (comma-delimited: id, URL prefix);
+ an FST mapping file to map individual URLs to unique integer ids (from above);

Then run this MapReduce program:

```
$ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar \
    org.warcbase.analysis.graph.ExtractSiteLinks \
    -hdfs /hdfs/path/to/data -output output \
    -numReducers 1 -urlMapping fst.dat -prefixFile prefix.csv
```

You'll find site-level webgraph in `output/` on HDFS.


Pig Integration
---------------

Warcbase comes with Pig integration for manipulating web archive data. Start the `Grunt` shell by typing `pig`. You should then be able to run the following Pig script to extract links:

```
register 'target/warcbase-0.1.0-SNAPSHOT-fatjar.jar';

DEFINE ArcLoader org.warcbase.pig.ArcLoader();
DEFINE ExtractLinks org.warcbase.pig.piggybank.ExtractLinks();

raw = load '/hdfs/path/to/data' using ArcLoader as
  (url: chararray, date: chararray, mime: chararray, content: bytearray);

a = filter raw by mime == 'text/html';
b = foreach a generate url, FLATTEN(ExtractLinks((chararray) content));

store b into '/output/path/';
```

In the output directory, you should find data output files with source URL, target URL, and anchor text.


License
-------

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).


Acknowledgments
---------------

This work is supported in part by the National Science Foundation and by the Mellon Foundation (via Columbia University). Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the sponsors.
