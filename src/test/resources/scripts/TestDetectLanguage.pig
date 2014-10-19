-- Simple language detection example

DEFINE ArcLoader org.warcbase.pig.ArcLoader();
DEFINE ExtractRawText org.warcbase.pig.piggybank.ExtractRawText();
DEFINE DetectLanguage org.warcbase.pig.piggybank.DetectLanguage();

raw = load '$testArcFolder' using ArcLoader();
-- schema is (url:chararray, date:chararray, mime:chararray, content:bytearray);

a = filter raw by mime == 'text/html';
b = foreach a generate url, mime,
    DetectLanguage(ExtractRawText((chararray) content)) as lang;
c = group b by lang;
d = foreach c generate group, COUNT(b);

dump d;
