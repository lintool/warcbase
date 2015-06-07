-- Counts up number of total records

DEFINE ArcLoader org.warcbase.pig.ArcLoader();

raw = load '$testArcFolder' using ArcLoader();
a = group raw all;
b = foreach a generate COUNT(raw);

store b into '$experimentfolder/counts' using PigStorage();
