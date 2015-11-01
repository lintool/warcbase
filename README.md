Warcbase
========

Warcbase is an open-source platform for managing web archives built on Hadoop and HBase. The platform provides a flexible data model for storing and managing raw content as well as metadata and extracted knowledge. Tight integration with Hadoop provides powerful tools for analytics and data processing via Spark.

There are two main ways of using Warcbase:

+ The first and most common is to analyze web archives using Spark (the preferred approach) or Pig (which is in the process of being deprecated).
+ The second is to take advantage of HBase to provide random access as well as analytics capabilities. Random access allows Warcbase to provide temporal browsing of archived content (i.e., "wayback" functionality).

You can use Warcbase without HBase, and since HBase requires more extensive setup, it is recommended that if you're just starting out, play with the Spark analytics and don't worry about HBase.

Warcbase is built against CDH 5.4.1:

+ Hadoop version: 2.6.0-cdh5.4.1
+ HBase version: 1.0.0-cdh5.4.1
+ Pig version: 0.12.0-cdh5.4.1
+ Spark version: 1.3.0-cdh5.4.1

The Hadoop ecosystem is evolving rapidly, so there may be incompatibilities with other versions.

Detailed documentation is available in [this repository's wiki](https://github.com/lintool/warcbase/wiki).


Getting Started
---------------

Clone the repo:

```
$ git clone http://github.com/lintool/warcbase.git
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


Spark Quickstart
----------------

For the impatient, let's do a simple analysis with Spark. Within the repo there's already a sample ARC file stored at `src/test/resources/arc/example.arc.gz`.

Assuming you've already got Spark installed, you can go ahead and fire up the Spark shell:

```
$ spark-shell --jars target/warcbase-0.1.0-SNAPSHOT-fatjar.jar
```

Here's a simple script that extracts the crawl date, domain, URL, and plain text from HTML files in the sample ARC data (and saves the output to `out/`):

```
import org.warcbase.spark.matchbox.ArcRecords
import org.warcbase.spark.matchbox.ArcRecords._

val r = ArcRecords.load("src/test/resources/arc/example.arc.gz", sc)
  .keepMimeTypes(Set("text/html"))
  .discardDate(null)
  .extractCrawldateDomainUrlBody()

r.saveAsTextFile("out/")
```

**Tip:** By default, commands in the Spark shell must be one line. To run multi-line commands, type `:paste` in Spark shell: you can then copy-paste the script above directly into Spark shell. Use Ctrl-D to finish the command.

What to learn more? Check out [analyzing web archives with Spark](https://github.com/lintool/warcbase/wiki/Analyzing-Web-Archives-with-Spark).


Next Steps
----------

+ [Ingesting content into HBase](https://github.com/lintool/warcbase/wiki/Ingesting-Content-into-HBase): loading ARC and WARC data into HBase
+ [Warcbase/Wayback integration](https://github.com/lintool/warcbase/wiki/Warcbase-Wayback-Integration): guide to provide temporal browsing capabilities
+ [Warcbase Java tools](https://github.com/lintool/warcbase/wiki/Warcbase-Java-Tools): building the URL mapping, extracting the webgraph


License
-------

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).


Acknowledgments
---------------

This work is supported in part by the National Science Foundation and by the Mellon Foundation (via Columbia University). Any opinions, findings, and conclusions or recommendations expressed are those of the researchers and do not necessarily reflect the views of the sponsors.
