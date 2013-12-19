-- Simple language detection example

--register 'target/warcbase-0.1.0-SNAPSHOT-fatjar.jar';

DEFINE ArcLoader org.warcbase.pig.ArcLoader();
DEFINE ExtractRawText org.warcbase.pig.piggybank.ExtractRawText();
DEFINE DetectLanguage org.warcbase.pig.piggybank.DetectLanguage();

raw = load '$testArcFolder'
    using ArcLoader() as (url: chararray, date:chararray, mime:chararray, content:chararray);

only_text = filter raw by (NOT(mime matches '^text.*'));

b = foreach only_text generate url, mime, ExtractRawText(content) as content;
-- c = foreach b generate url,mime,DetectLanguage(content) as lang;

--non_text = filter raw by (NOT(mime matches '^text.*'));

c1 = foreach b generate DetectLanguage(content) as lang;

d = group c1 by lang;
g = foreach d generate group, COUNT(c1);

store e into '$experimentfolder/e';
-- dump e;