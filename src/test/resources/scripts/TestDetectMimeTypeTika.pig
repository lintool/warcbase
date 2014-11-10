-- Combined mime type check and language detection on an arc file

define ArcLoader org.warcbase.pig.ArcLoader();
define DetectMimeTypeTika org.warcbase.pig.piggybank.DetectMimeTypeTika();

raw = load '$testArcFolder' using ArcLoader();
-- schema is (url:chararray, date:chararray, mime:chararray, content:bytearray);

-- Detect the mime type of the content using and Tika
a = foreach raw generate url,mime, DetectMimeTypeTika(content) as tikaMime;

tikaMimes      = foreach a generate tikaMime;
tikaMimeGroups = group tikaMimes by tikaMime;
tikaMimeBinned = foreach tikaMimeGroups generate group, COUNT(tikaMimes);

dump tikaMimeBinned;

store tikaMimeBinned into '$experimentfolder/tikaMimeBinned';

