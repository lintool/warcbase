-- Simple word count example to tally up dates when pages are crawled

DEFINE ArcLoader org.warcbase.pig.ArcLoader();

raw = load '$testArcFolder' using ArcLoader();
-- schema is (url:chararray, date:chararray, mime:chararray, content:bytearray);

store raw into '$experimentfolder/raw' using PigStorage();

a = foreach raw generate SUBSTRING(date, 0, 8) as date;
b = group a by date;
c = foreach b generate group, COUNT(a);

store c into '$experimentfolder/c' using PigStorage();