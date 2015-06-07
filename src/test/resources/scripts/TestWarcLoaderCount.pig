-- Counts up number of total records

DEFINE WarcLoader org.warcbase.pig.WarcLoader();

raw = load '$testWarcFolder' using WarcLoader();
a = group raw all;
b = foreach a generate COUNT(raw);

store b into '$experimentfolder/counts' using PigStorage();
