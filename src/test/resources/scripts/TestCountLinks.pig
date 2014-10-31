-- Simple word count example to tally up dates when pages are crawled

DEFINE ArcLoader org.warcbase.pig.ArcLoader();

raw = load '$testArcFolder' using ArcLoader();
-- schema is (url:chararray, date:chararray, mime:chararray, content:bytearray);

a = foreach raw generate FLATTEN(org.warcbase.pig.piggybank.ExtractLinks((chararray) content));

store a into '$experimentfolder/a';