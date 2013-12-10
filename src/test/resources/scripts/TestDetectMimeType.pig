
-- Combined mime type check and language detection on an arc file
--register 'target/warcbase-0.1.0-SNAPSHOT-fatjar.jar';

define ArcLoader org.warcbase.pig.ArcLoader();
define DetectMimeType org.warcbase.pig.piggybank.DetectMimeType();

-- Load arc file properties: url, date, mime, content
raw = load '$testArcFolder' using org.warcbase.pig.ArcLoader() as (url: chararray, date:chararray, mime:chararray, content:chararray);

-- only identify text based formats
--non_text = filter raw by (NOT(mime matches '^text.*'));
--set_to_detect = non_text;

-- identify all formats
set_to_detect = raw;

-- Detect the mime type of the content using magic lib and Tika
a = foreach set_to_detect generate url,mime, DetectMimeType(content, 'magic') as magicMime, DetectMimeType(content, 'tika') as tikaMime;

-- magic lib includes "; <char set>" in which we are not interested
b = foreach a {
    magicMimeSplit = STRSPLIT(magicMime, ';');
    GENERATE url, mime, magicMimeSplit.$0, tikaMime;
}

httpMimes = foreach a generate mime;
httpMimeGroups = group httpMimes by mime;
httpMimeBinned = foreach httpMimeGroups generate group, COUNT(httpMimes);
dump httpMimeBinned;

store b into '$experimentfolder/b';

--dump a;
