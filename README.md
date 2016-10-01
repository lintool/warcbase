Warcbase [![Build Status](https://travis-ci.org/lintool/warcbase.svg?branch=master)](https://travis-ci.org/lintool/warcbase)
========

Warcbase is an open-source platform for managing web archives built on Hadoop and HBase. The platform provides a flexible data model for storing and managing raw content as well as metadata and extracted knowledge. Tight integration with Hadoop provides powerful tools for analytics and data processing via Spark.

There are two main ways of using Warcbase:

+ The first and most common is to analyze web archives using [Spark](http://spark.apache.org/): these functionalities are contained in the `warcbase-core` module.
+ The second is to take advantage of HBase to provide random access as well as analytics capabilities. Random access allows Warcbase to provide temporal browsing of archived content (i.e., "wayback" functionality): these functionalities are contained in the `warcbase-hbase` module.

You can use Warcbase without HBase, and since HBase requires more extensive setup, it is recommended that if you're just starting out, play with the Spark analytics and don't worry about HBase.

Other helpful links:

+ Detailed documentation is available [here](http://lintool.github.io/warcbase-docs/).
+ Supporting files can be found in the [warcbase-resources repository](https://github.com/lintool/warcbase-resources).

Getting Started
---------------

Clone the repo:

```
$ git clone http://github.com/lintool/warcbase.git
```

You can then build Warcbase. If you are just interested in the analytics function, you can run the following:

```
$ mvn clean package -pl warcbase-core
```

For the impatient, to skip tests:

```
$ mvn clean package -pl warcbase-core -DskipTests
```

If you are interested in the HBase functionality as well, you can build everything using:

```
$ mvn clean package
```

Warcbase is built against CDH 5.7.1:

+ Hadoop version: 2.6.0-cdh5.7.1
+ Spark version: 1.6.0-cdh5.7.1
+ HBase version: 1.2.0-cdh5.7.1

The Hadoop ecosystem is evolving rapidly, so there may be incompatibilities with other versions.

Spark Quickstart
----------------

For the impatient, let's do a simple analysis with Spark. Within the repo there's already a sample ARC file stored at `warcbase-core/src/test/resources/arc/example.arc.gz`. Our supporting resources repository also has [larger ARC and WARC files as real-world examples](https://github.com/lintool/warcbase-resources/tree/master/Sample-Data).

If you need to install Spark, [we have a walkthrough here](http://lintool.github.io/warcbase-docs/Getting-Started/). This page also has instructions on how to install and run Spark Notebook, an interactive web-based editor.

Once you've got Spark installed, go ahead and fire up the Spark shell:

```
$ spark-shell --jars warcbase-core/target/warcbase-core-0.1.0-SNAPSHOT-fatjar.jar
```

Here's a simple script that extracts and counts the top-level domains (i.e., number of pages for each top-level domain) in the sample ARC data:

```scala
import org.warcbase.spark.matchbox._
import org.warcbase.spark.rdd.RecordRDD._

val r = RecordLoader.loadArchives("warcbase-core/src/test/resources/arc/example.arc.gz", sc)
  .keepValidPages()
  .map(r => ExtractDomain(r.getUrl))
  .countItems()
  .take(10)
```

**Tip:** By default, commands in the Spark shell must be one line. To run multi-line commands, type `:paste` in the Spark shell: you can then copy-paste the script above directly into Spark shell. Use Ctrl-D to finish the command.

What to learn more? Check out our [detailed documentation](http://lintool.github.io/warcbase-docs/).


Visualizations
--------------

The result of analyses of using Warcbase can serve as input to visualizations that help scholars interactively explore the data. Examples include:

+ [Basic crawl statistics](http://lintool.github.io/warcbase/vis/crawl-sites/index.html) from the Canadian Political Parties and Political Interest Groups collection.
+ [Interactive graph visualization](http://lintool.github.io/warcbase-docs/Gephi-Converting-Site-Link-Structure-into-Dynamic-Visualization/) using Gephi.
+ [Named entity visualization](http://lintool.github.io/warcbase-docs/Spark-NER-Visualization/) for exploring relative frequencies of people, places, and locations.
+ [Shine interface](http://webarchives.ca/) for faceted full-text search.


Next Steps
----------

+ [Ingesting content into HBase](http://lintool.github.io/warcbase-docs/Ingesting-Content-into-HBase/): loading ARC and WARC data into HBase
+ [Warcbase/Wayback integration](http://lintool.github.io/warcbase-docs/Warcbase-Wayback-Integration/): guide to provide temporal browsing capabilities
+ [Warcbase Java tools](http://lintool.github.io/warcbase-docs/Warcbase-Java-Tools/): building the URL mapping, extracting the webgraph


License
-------

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).


Acknowledgments
---------------

This work is supported in part by the U.S. National Science Foundation, the Natural Sciences and Engineering Research Council of Canada, the Social Sciences and Humanities Research Council of Canada, the Ontario Ministry of Research and Innovation's Early Researcher Award program, and the Mellon Foundation (via Columbia University). Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the sponsors.

