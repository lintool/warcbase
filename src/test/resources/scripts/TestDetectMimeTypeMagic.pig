
-- Combined mime type check and language detection on an arc file
--register 'target/warcbase-0.1.0-SNAPSHOT-fatjar.jar';

define ArcLoader org.warcbase.pig.ArcLoader();
define DetectMimeTypeMagic org.warcbase.pig.piggybank.DetectMimeTypeMagic();

-- Load arc file properties: url, date, mime, content
raw = load '$testArcFolder' using org.warcbase.pig.ArcLoader() as (url: chararray, date:chararray, mime:chararray, content:chararray);

-- Detect the mime type of the content using magic lib
-- On CentOS the magic file is located at /usr/share/file/magic.mgc
-- On MacOS X using Homebrew the magic file is located at /usr/local/Cellar/libmagic/5.15/share/misc/magic.mgc
a = foreach raw generate url,mime, DetectMimeTypeMagic('/usr/local/Cellar/libmagic/5.15/share/misc/magic.mgc', content) as magicMime;


-- magic lib includes "; <char set>" in which we are not interested
b = foreach a {
    magicMimeSplit = STRSPLIT(magicMime, ';');
    GENERATE url, mime, magicMimeSplit.$0 as magicMime;
}

-- httpMimes      = foreach b generate mime;
-- httpMimeGroups = group httpMimes by mime;
-- httpMimeBinned = foreach httpMimeGroups generate group, COUNT(httpMimes);

magicMimes      = foreach b generate magicMime;
magicMimeGroups = group magicMimes by magicMime;
magicMimeBinned = foreach magicMimeGroups generate group, COUNT(magicMimes);

--dump httpMimeBinned;
--dump tikaMimeBinned;
--dump magicMimeBinned;

-- store httpMimeBinned into '$experimentfolder/httpMimeBinned';
store magicMimesBinned into '$experimentfolder/magicMimeBinned';

