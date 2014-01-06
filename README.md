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

Implemented Pig UDFs
---------------

For examples of how to use these UDF see in `src/test/resources/scripts`

### DetectLanguage

This UDF uses Apache Tika to detect the language of the given text.

### DetectMimeTypeTika

This UDF uses Apache Tika to detect the MIME type of the provided data stream.

### DetectMimeTypeMagic

This UDF uses the magic-lib from the UNIX file command to detect the MIME type of the provided data stream. The function relies on the presence of the libmagic library and the dso-called magic file. As these locations are platform dependent, the path to the magic file shall be provided when calling the function and the library shall be in the LIBPATH (Not sure whether this last statement is correct).

### ExtractLinks

UDF for extracting links from a webpage given the HTML content (using Jsoup). Returns a bag of tuples, where each tuple consists of the URL and the anchor text.

### ExtractRawText

UDF for extracting raw text content from an HTML page (using Jsoup).

### ArcLoader and WarcLoader

UDF providing loading ARC and WARC files in Pig Latin


