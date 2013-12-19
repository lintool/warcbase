WarcBase
========

A web archive browser built on HBase.

Getting Started
---------------

Once you check out the repo, build WarcBase:

```
mvn clean package appassembler:assemble
```

Ingesting WARC files:

```
$ setenv CLASSPATH_PREFIX "/etc/hbase/conf/"
$ sh target/appassembler/bin/IngestWarcFiles -dir /path/to/warc/ -name archive_name -create
```

Command-line options:

+ Use the `-dir` option to specify directory containing WARC files.
+ Use the `-name` option to specify the name of the archive (will correspond to the HBase table name).
+ Use the `-create` option to create a new table (and drop the existing table if a table with the same name exists already). Alternatively, use `-append` to add to an existing table.

Starting the browser:

```
$ setenv CLASSPATH_PREFIX "/etc/hbase/conf/"
$ sh target/appassembler/bin/WarcBrowser -port 9191 -server http://myhost:9191/
```

Navigate to `http://myhost:9191/` to browse the archive.

## Development notes

2013-12-11
An ARC files with 2224 records was identified in 15 minutes on a MacBook Air.