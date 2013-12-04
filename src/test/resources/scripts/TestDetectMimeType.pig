
-- Combined mime type check and language detection on an arc file
--register 'target/warcbase-0.1.0-SNAPSHOT-fatjar.jar';

define ArcLoader org.warcbase.pig.ArcLoader();
define ExtractRawText org.warcbase.pig.piggybank.ExtractRawText();
define DetectMimeType org.warcbase.pig.piggybank.DetectMimeType();

-- Load arc file properties: url, date, mime, content
raw = load '$testArcFolder' using
  org.warcbase.pig.ArcLoader() as (url: chararray, date:chararray, mime:chararray, content:chararray);

-- Detect the mime type of the content using magic lib
a = foreach raw generate url,mime, DetectMimeType(content) as fileMime;

-- store d into 'tmp' using PigStorage();
store a into '$experimentfolder/a';
--dump a;
