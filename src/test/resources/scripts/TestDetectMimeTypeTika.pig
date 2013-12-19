
-- Combined mime type check and language detection on an arc file
--register 'target/warcbase-0.1.0-SNAPSHOT-fatjar.jar';

define ArcLoader org.warcbase.pig.ArcLoader();
define DetectMimeTypeTika org.warcbase.pig.piggybank.DetectMimeTypeTika();

-- Load arc file properties: url, date, mime, content
raw = load '$testArcFolder' using org.warcbase.pig.ArcLoader() as (url: chararray, date:chararray, mime:chararray, content:chararray);

-- Detect the mime type of the content using and Tika
a = foreach raw generate url,mime, DetectMimeTypeTika(content) as tikaMime;


tikaMimes      = foreach a generate tikaMime;
tikaMimeGroups = group tikaMimes by tikaMime;
tikaMimeBinned = foreach tikaMimeGroups generate group, COUNT(tikaMimes);

--dump httpMimeBinned;
--dump tikaMimeBinned;
--dump magicMimeBinned;

store magicMimesBinned into '$experimentfolder/magicMimeBinned';

