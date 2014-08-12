-- Simple word count example to tally up dates when pages are crawled

--register 'target/warcbase-0.1.0-SNAPSHOT-fatjar.jar';

DEFINE ArcLoader org.warcbase.pig.ArcLoader();

raw = load '$testArcFolder' using ArcLoader as (url: chararray, date:chararray, mime:chararray, content:chararray);

a = foreach raw generate FLATTEN(org.warcbase.pig.piggybank.ExtractLinks(content));

store a into '$experimentfolder/a';