# Building and Running Warcbase under OS X

In order to build a functional Warcbase setup under OS X, a number of minor changes to the [official project instructions](https://github.com/lintool/warcbase) are required.

### Prerequisites
* [OS X Developer Tools](http://www.cnet.com/how-to/install-command-line-developer-tools-in-os-x/)
* [Homebrew](http://brew.sh)
* Maven (`brew install maven`)
* Hadoop (`brew install hadoop`)
* HBase (`brew install hbase`)
    
    Configure HBase by making the changes to the following files located in the HBase installation directory, which will be something like `/usr/local/Cellar/hbase/0.98.6.1/libexec/` (depending on the version number).
    
    * `conf/hbase-site.xml`:
    Insert within the <configuration></configuration> tags
    <property>
        <name>hbase.rootdir</name>
        <value>file:///Users/yourname/hbase</value>
    </property>
    <property>
        <name>hbase.zookeeper.property.dataDir</name>
        <value>/Users/yourname/zookeeper</value>
    </property>
  
    Where `yourname` is your username. Feel free to choose other directories to store these files, used by HBase and its ZooKeeper instance, if you like. *HBase will create these directories. If they already exist, they will cause problems later on.*
    
    * `conf/hbase-env.html`: Look for the following line,
          export HBASE_OPTS="-XX:+UseConcMarkSweepGC"
        
    and change it to:
    
          export HBASE_OPTS="-XX:+UseConcMarkSweepGC -Djava.security.krb5.realm=-Djava.security.krb5.kdc="
        
    Verify that HBase is installed correctly by running the HBase shell:
    
        $ hbase shell
        
        hbase(main):001:0> list
        
        // Some lines of log messages
        
        0 row(s) in 1.3060 seconds
        
        => []
        
        hbase(main):002:0> exit
    
* Tomcat (`brew install tomcat`)

* Pig (`brew install pig`)


> **_N.B._** If you run an automatic Homebrew system update (`brew update && brew upgrade`) it is possible a new version of Hadoop, HBase, or Tomcat will be installed. The previous version will remain on your system, but the symbolic links in `/ur/local/bin/` will point to the new version; i.e., it is the new version that will be executed when you run any of the software's components, unless you specify the full pathname. There are two solutions:
> 1. Re-configure the new version of the updated software according to the instructions above.
> 2. Make the symbolic links point to the older version of the updated software, with the command `brew switch <formula> <version>`. E.g., `brew switch hbase 0.98.6.1`.


### Building Warcbase

To start, you will need to clone the Warcbase Git repository:
    
    $ git clone http://github.com/lintool/warcbase.git
    
From inside the root directory `warcbase`, build the project:

    $ mvn clean package appassembler:assemble -DskipTests

If you leave off `-DskipTests`, the build may fail when it runs tests due to a shortage of memory. If you try the build with the tests and this happens, don't worry about it. 

Because OS X is not quite case sensitive (it does not allow two files or directories spelled the same but for case), you must remove one file from the JAR package:

    $ zip -d target/warcbase-0.1.0-SNAPSHOT-fatjar.jar META-INF/LICENSE
    
### Ingesting content
To ingest a directory of web archive files (which may be GZip-compressed, e.g., webcollection.arc.gz), run the following from inside the `warcbase` directory. 

    $ start-hbase.sh
    $ export CLASSPATH_PREFIX="/usr/local/Cellar/hbase/0.98.6.1/libexec/conf/"
    $ sh target/appassembler/bin/IngestFiles -dir /path/to/webarchive/files/ -name archive_name -create -gz
    
Change as appropriate the HBase configuration path (version number), the directory of web archive files, and the archive name. Use the option `-append` instead of `-create` to add to an existing database table. Note the `-gz` flag: this changes compression method to Gzip from the default Snappy, which is unavailable as a native Hadoop library on OS X. (The above commands assume you are using a shell in the `bash` family.)

*Tip*: To avoid repeatedly setting the CLASSPATH_PREFIX variable, add the `export` line to your `~/.bash_profile` file.

If you wish to shut down HBase, the command is `stop-hbase.sh`. You can check if HBase is running with the command `jps`; if it is running you will see the process `HMaster` listed. You can also view detailed server status information at http://localhost:60010/.

### Run and test the WarcBrowser
You may now view your archived websites through the WarcBrowser interface.

    # Start HBase first, if it isn't already running:
    $ start-hbase.sh
    # Set CLASSPATH_PREFIX, if it hasn't been done this terminal session:
    $ export CLASSPATH_PREFIX="/usr/local/Cellar/hbase/0.98.6.1/libexec/conf/"
    # Start the browser:
    $ sh target/appassembler/bin/WarcBrowser -port 8079
    
You can now use `http://localhost:8079/` to browse the archive. For example:
* `http://localhost:8079/archive_name/*/http://mysite.com/` will give you a list of available versions of `http://mysite.com/`.
* `http://localhost:8079/archive_name/19991231235959/http://mysite.com/` will give you the record of `http://mysite.com/` just before Y2K.

### OpenWayback Integration
For a more functional, visually-appealing interface, you may install a custom version of the Wayback Machine. 

Assuming Tomcat is installed, start it by running:
    
    $ catalina start

Install OpenWayback by downloading the latest binary release [here](https://github.com/iipc/openwayback/wiki/How-to-install). Extract the .tar.gz; inside it there will be a web application file `openwayback-(version).war`. Copy this file into the `webapps` folder of Tomcat, something like `/usr/local/Cellar/tomcat/8.0.17/libexec/webapps/`, and rename it `ROOT.war`. Tomcat will immediately unpack this file into the `ROOT` directory.

*If you are running a current version of Tomcat (i.e., version 8) in combination with OpenWayback 2.0.0, edit the file `webapps/ROOT/WEB-INF/web.xml` and insert a slash ("/") in front of the paths of parameters `logging-config-path` and `config-path`. ([Details](https://github.com/iipc/openwayback/issues/196)) Future releases of OpenWayback should already include this configuration change.*

Add the Warcbase jar file to the Wayback installation, by copying `target/appassembler/repo/org/warcbase/warcbase/0.1.0-SNAPSHOT/warcbase-0.1.0-SNAPSHOT.jar` from the Warcbase build directory into Tomcat's `webapps/ROOT/WEB-INF/lib/`. 

Into `webapps/ROOT/WEB-INF` copy Warcbase's `src/main/resources/BDBCollection.xml`. In `BDBCollection.xml`, replace `HOST`, `PORT`, and `TABLE` with `localhost`, `8079`, and `archive_name` (or whatever the archive table in HBase is called).

Restart Tomcat:

    $ catalina stop
    $ catalina start
    
Now, navigate to http://localhost:8080/wayback/ and access one of your archived web pages through the Wayback interface.

## Analytics
Warcbase is useful for managing web archives, but its real power is as a platform for processing and analyzing the archives in its database. Its analysis tools are still under development, but at the moment you can use the tools described below, including the _pig_ scripting interface.

### Building the URL mapping
Most of the tools that follow require a URL mapping file, which maps every URL in a set of ARC/WARC files to a unique integer ID. There are two ways of doing this; the first is simpler:

    $ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar org.warcbase.data.UrlMappingMapReduceBuilder -input /path/to/webarc/files -output fst.dat

If this does not work due to a lack of memory, try the following steps:

    $ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar org.warcbase.analysis.ExtractUniqueUrls -input /path/to/webarchive/files -output urls 
    
(If you have configured HBase to run in distributed mode rather than in standalone mode, which is the configuration provided above, you must now copy the `urls` directory out of HDFS into the local filesystem.)

Next:

    $ sh target/appassembler/bin/UrlMappingBuilder -input /path/to/urls -output fst.dat
    
We can examine the FST data with the following utility program:

    # Lookup by URL, fetches the integer id
    $ sh target/appassembler/bin/UrlMapping -data fst.dat -getId http://www.foo.com/
    # Lookup by id, fetches the URL
    $ sh target/appassembler/bin/UrlMapping -data fst.dat -getUrl 42
    # Fetches all URLs with the prefix
    $ sh target/appassembler/bin/UrlMapping -data fst.dat -getPrefix http://www.foo.com/    


(If you are running in distributed mode, now copy the `fst.dat` file into the HDFS, so it is accessible to the cluster:

    $ hadoop fs -put fst.dat /hdfs/path/
)

> You might have noticed, we are working here with ARC/WARC files and not tables in HBase. The same is done below as well. This is because most of the tools described her and below do not yet have HBase support.

### Extracting the webgraph
We can use the mapping data from above to extract the webgraph, with a Hadoop program:

    $ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar org.wa
    rcbase.analysis.graph.ExtractLinksWac -hdfs /path/to/webarchive/files -output output -urlMapping fst.dat 

The `-hdfs` flag is misleading; if HBase is running in standalone mode, this flag should specifiy a local path. 

The resulting webgraph will appear in the `output` directory, in one or more files with names like `part-m-00000`, `part-m-00001`, etc.

### Extracting a site-level webgraph
Instead of extracting links between individual URLs, we can extract the site-level webgraph by aggregating all URLs with common prefix into a "supernode". Link counts between supernodes represent the total number of links between individual URLs. In order to do this, the following input files are needed:
* a CSV prefix file providing URL prefixes for each supernode (comma-delimited: ID, URL prefix). The first line of this file is ignored (reserved for headers). The URL prefix is a simple string representing a site, e.g., `http://cnn.com/`. The ID should be unique (I think), so take not of the total number of unique URLs extracted when building the URL mapping above, and make sure your IDs are larger than this.
* an FST mapping file to map individual URLs to unique integer ids (from above)

Then run this MapReduce program:

    $ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar org.warcbase.data.ExtractSiteLinks -hdfs /path/to/webarchive/files -output output -numReducers 1 -urlMapping fst.dat -prefixFile prefix.csv
    
### Other analysis tools
The tools described in this section are relatively simple. Some can process ARC and/or WARC files, while others exist in ARC and WARC versions.

There are three counting tools. They count different content-types, crawl-dates, and urls:
    
    $ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar  org.warcbase.analysis.CountArcContentTypes -input /arc/files/ -output contentTypes

    $ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar  org.warcbase.analysis.CountArcCrawlDates -input /arc/files/ -output crawlDates
    
    $ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar  org.warcbase.analysis.CountArcUrls -input ~/arc/files/ -output urls

For WARC files, replace "Arc" with "Warc" in the class names (e.g., `org.warcbase.analysis.CountArcContentTypes` becomes `org.warcbase.analysis.CountWarcContentTypes`).

There is a tool to extract unique URLs:

    $ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar org.warcbase.analysis.ExtractUniqueUrls -input /arc/or/warc/files -output uniqueUrls

There is a pair of tools to find URLs, according to a regex pattern. 

    $ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar  org.warcbase.analysis.FindArcUrls -input /arc/files/ -output foundUrls -pattern "http://.*org/.*"

    $ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar  org.warcbase.analysis.FindWarcUrls -input /warc/files/ -output foundUrls -pattern "http://.*org/.*"
    
There is a tool to detect duplicates in the HBase:

    $ sh target/appassembler/bin/DetectDuplicates -name table

There is also a web graph tool that pulls out link anchor text (background: https://github.com/lintool/warcbase/issues/8). 

    $ hadoop jar target/warcbase-0.1.0-SNAPSHOT-fatjar.jar  org.warcbase.analysis.graph.InvertAnchorText -hdfs /arc/or/warc/files -output output -numReducers 1 -urlMapping fst.dat

For all of these tools that employ a URL mapping, you must use the FST mapping generated for the set of data files you are analyzing.

### Pig integration
Warcbase comes with Pig integration for manipulating web archive data. Pig scripts may be run from the interactive Grunt shell (run `pig`), or, more conveniently, from a file (e.g., `pig -f extractlinks.pig`).

The following script extracts links:
````
register 'target/warcbase-0.1.0-SNAPSHOT-fatjar.jar';

DEFINE ArcLoader org.warcbase.pig.ArcLoader();
DEFINE ExtractLinks org.warcbase.pig.piggybank.ExtractLinks();

raw = load '/path/to/arc/files' using ArcLoader as
  (url: chararray, date: chararray, mime: chararray, content: bytearray);

a = filter raw by mime == 'text/html';
b = foreach a generate url, FLATTEN(ExtractLinks((chararray) content));

store b into '/output/path/';
````
In the output directory you should find data output files with source URL, target URL, and anchor text.
