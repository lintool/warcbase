-- Counts up number of links

DEFINE ArcLoader org.warcbase.pig.ArcLoader();

raw = load '$testArcFolder' using ArcLoader();
a = foreach raw generate FLATTEN(org.warcbase.pig.piggybank.ExtractLinks((chararray) content));

store a into '$experimentfolder/a';
